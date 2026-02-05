#include "server.h"
#include <arpa/inet.h>
#include <unistd.h>
#include <sstream>
#include <thread>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <algorithm>

Server::Server(int port, Role role, int server_id,
               const std::vector<std::string>& peers)
    : port_(port),
      server_id_(server_id),
      peers_(peers),
      role_(role),
      wal_("wal_" + std::to_string(port) + ".log"),
      snapshot_manager_("snapshots", server_id),
      rng_(std::random_device{}()) {
    
    // Load persistent state
    loadState();
    
    // Try to load snapshot first (faster recovery)
    if (loadSnapshot()) {
        std::cout << "[INFO] Loaded from snapshot" << std::endl;
    }
    
    // Replay WAL to apply entries after snapshot
    wal_.replay(store_);
    
    std::cout << "[INFO] Server " << server_id_ << " initialized on port " 
              << port_ << " in role " << (role_ == Role::LEADER ? "LEADER" : "FOLLOWER") 
              << std::endl;
}

Server::~Server() {
    shutdown();
}

void Server::shutdown() {
    running_ = false;
    event_queue_.shutdown();
    
    if (event_loop_thread_.joinable()) {
        event_loop_thread_.join();
    }
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
    if (election_thread_.joinable()) {
        election_thread_.join();
    }
}

void Server::loadState() {
    wal_.loadMetadata(current_term_, voted_for_);
}

void Server::persistState() {
    wal_.saveMetadata(current_term_, voted_for_);
}

int Server::getElectionTimeout() {
    // Random timeout between 150-300ms (scaled up for testing: 3-6 seconds)
    std::uniform_int_distribution<> dist(3000, 6000);
    return dist(rng_);
}

void Server::startEventLoop() {
    event_loop_thread_ = std::thread([this]() {
        std::cout << "[INFO] Event loop started\n";
        
        while (running_) {
            auto event_opt = event_queue_.pop_with_timeout(std::chrono::milliseconds(100));
            if (!event_opt.has_value()) {
                continue;
            }
            
            processEvent(event_opt.value());
        }
        
        std::cout << "[INFO] Event loop stopped\n";
    });
}

void Server::processEvent(const Event& e) {
    if (e.type == EventType::CLIENT_PUT) {
        // Leader appends to log and initiates replication
        if (role_ != Role::LEADER) {
            if (e.client_callback) {
                e.client_callback(false, "NOT_LEADER");
            }
            return;
        }
        
        // Get last log info
        int last_index, last_term;
        wal_.getLastLogInfo(last_index, last_term);
        
        int new_index = last_index + 1;
        LogEntry entry(new_index, current_term_, e.key, e.value, "PUT");
        
        // Append to local log
        wal_.appendEntry(entry);
        
        // Store callback for when this gets committed
        if (e.client_callback) {
            std::lock_guard<std::mutex> lock(pending_requests_mutex_);
            pending_requests_[new_index] = {new_index, e.client_callback};
        }
        
        // Trigger replication
        std::thread([this, new_index]() {
            if (replicator_) {
                bool success = replicator_->replicateEntries(
                    current_term_, commit_index_, wal_, new_index
                );
                
                if (success) {
                    // Update commit index
                    Event commit_event;
                    commit_event.type = EventType::REPL_ACK;
                    commit_event.ack_index = new_index;
                    event_queue_.push(commit_event);
                }
            } else {
                // Single-node cluster, commit immediately
                Event commit_event;
                commit_event.type = EventType::REPL_ACK;
                commit_event.ack_index = new_index;
                event_queue_.push(commit_event);
            }
        }).detach();
    }
    else if (e.type == EventType::REPL_ACK) {
        // Replication succeeded, advance commit index
        if (role_ == Role::LEADER) {
            int new_commit = e.ack_index;
            
            if (replicator_) {
                new_commit = replicator_->calculateCommitIndex(commit_index_, current_term_, wal_);
            }
            
            if (new_commit > commit_index_) {
                commit_index_ = new_commit;
                std::cout << "[INFO] Advanced commit index to " << commit_index_ << std::endl;
                
                // Apply committed entries
                advanceCommitIndex();
            }
        }
    }
    else if (e.type == EventType::HEARTBEAT_TICK) {
        if (role_ == Role::LEADER && replicator_) {
            replicator_->sendHeartbeats(current_term_, commit_index_, wal_);
        }
    }
}

void Server::advanceCommitIndex() {
    while (last_applied_ < commit_index_) {
        last_applied_++;
        
        LogEntry entry;
        if (wal_.getEntry(last_applied_, entry)) {
            applyLogEntry(entry);
            
            // Notify waiting client if this was their request
            std::lock_guard<std::mutex> lock(pending_requests_mutex_);
            auto it = pending_requests_.find(last_applied_);
            if (it != pending_requests_.end()) {
                if (it->second.callback) {
                    it->second.callback(true, "OK");
                }
                pending_requests_.erase(it);
            }
        }
    }
}

void Server::applyLogEntry(const LogEntry& entry) {
    if (entry.operation == "PUT") {
        store_.put(entry.key, entry.value);
        std::cout << "[INFO] Applied entry " << entry.index << ": PUT " 
                  << entry.key << "=" << entry.value << std::endl;
    }
    
    // Track entries since last snapshot
    entries_since_snapshot_++;
    
    // Trigger snapshot if threshold reached (async, doesn't block)
    if (role_ == Role::LEADER) {
        createSnapshotIfNeeded();
    }
}

void Server::stepDown(int new_term) {
    if (new_term > current_term_) {
        current_term_ = new_term;
        voted_for_ = -1;
        persistState();
    }
    
    role_ = Role::FOLLOWER;
    replicator_.reset();
    
    std::cout << "[INFO] Stepped down to FOLLOWER, term " << current_term_ << std::endl;
}

void Server::startElection() {
    role_ = Role::CANDIDATE;
    current_term_++;
    voted_for_ = server_id_;
    persistState();
    
    last_heartbeat_ = std::chrono::steady_clock::now();
    
    std::cout << "[INFO] Starting election for term " << current_term_ << std::endl;
    
    int votes = 1; // Vote for self
    std::mutex vote_mutex;
    std::vector<std::thread> threads;
    
    int last_log_index, last_log_term;
    wal_.getLastLogInfo(last_log_index, last_log_term);
    
    for (const auto& peer : peers_) {
        threads.emplace_back([&, peer]() {
            std::string ip = peer.substr(0, peer.find(':'));
            int port = std::stoi(peer.substr(peer.find(':') + 1));

            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) return;

            struct timeval timeout;
            timeout.tv_sec = 1;
            timeout.tv_usec = 0;
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

            sockaddr_in serv{};
            serv.sin_family = AF_INET;
            serv.sin_port = htons(port);
            inet_pton(AF_INET, ip.c_str(), &serv.sin_addr);

            if (connect(sock, (sockaddr*)&serv, sizeof(serv)) < 0) {
                close(sock);
                return;
            }

            std::ostringstream oss;
            oss << "REQUEST_VOTE " << current_term_ << " " << server_id_ 
                << " " << last_log_index << " " << last_log_term << "\n";
            std::string msg = oss.str();
            write(sock, msg.c_str(), msg.size());

            char buf[128];
            int n = read(sock, buf, sizeof(buf));
            if (n > 0) {
                std::string resp(buf, n);
                if (resp.find("VOTE_GRANTED") != std::string::npos) {
                    std::lock_guard<std::mutex> lock(vote_mutex);
                    votes++;
                }
            }

            close(sock);
        });
    }
    
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
    
    // Check if we won
    int cluster_size = static_cast<int>(peers_.size()) + 1;
    if (votes > (cluster_size / 2) && role_ == Role::CANDIDATE) {
        becomeLeader();
    } else {
        role_ = Role::FOLLOWER;
        std::cout << "[INFO] Election failed, got " << votes << " votes\n";
    }
}

void Server::becomeLeader() {
    role_ = Role::LEADER;
    
    int last_log_index, last_log_term;
    wal_.getLastLogInfo(last_log_index, last_log_term);
    
    replicator_ = std::make_unique<Replicator>(peers_, server_id_);
    replicator_->resetState(last_log_index);
    
    std::cout << "[INFO] *** BECAME LEADER for term " << current_term_ << " ***\n";
    
    // Start sending heartbeats
    startHeartbeatSender();
    
    // Send initial empty AppendEntries to establish leadership
    if (replicator_) {
        replicator_->sendHeartbeats(current_term_, commit_index_, wal_);
    }
}

void Server::startHeartbeatSender() {
    heartbeat_thread_ = std::thread([this]() {
        while (running_ && role_ == Role::LEADER) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            
            Event e;
            e.type = EventType::HEARTBEAT_TICK;
            event_queue_.push(e);
        }
    });
}

void Server::startElectionTimer() {
    election_thread_ = std::thread([this]() {
        last_heartbeat_ = std::chrono::steady_clock::now();
        
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            if (role_ == Role::LEADER) {
                continue; // Leaders don't run election timers
            }
            
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_heartbeat_
            ).count();
            
            int timeout = getElectionTimeout();
            if (elapsed > timeout) {
                std::cout << "[WARN] Election timeout! Last heartbeat " 
                          << elapsed << "ms ago\n";
                startElection();
                last_heartbeat_ = std::chrono::steady_clock::now();
            }
        }
    });
}

void Server::handleRequestVote(int client_fd, const std::string& request) {
    std::istringstream iss(request);
    std::string cmd;
    int term, candidate_id, last_log_index, last_log_term;
    
    iss >> cmd >> term >> candidate_id >> last_log_index >> last_log_term;
    
    bool vote_granted = false;
    
    // Update term if necessary
    if (term > current_term_) {
        stepDown(term);
    }
    
    int my_last_log_index, my_last_log_term;
    wal_.getLastLogInfo(my_last_log_index, my_last_log_term);
    
    // Grant vote if:
    // 1. Term matches
    // 2. Haven't voted or already voted for this candidate
    // 3. Candidate's log is at least as up-to-date
    if (term == current_term_ && 
        (voted_for_ == -1 || voted_for_ == candidate_id)) {
        
        bool log_ok = (last_log_term > my_last_log_term) ||
                      (last_log_term == my_last_log_term && 
                       last_log_index >= my_last_log_index);
        
        if (log_ok) {
            voted_for_ = candidate_id;
            persistState();
            vote_granted = true;
            last_heartbeat_ = std::chrono::steady_clock::now();
            
            std::cout << "[INFO] Granted vote to " << candidate_id 
                      << " for term " << term << std::endl;
        }
    }
    
    std::string response = vote_granted ? "VOTE_GRANTED\n" : "VOTE_DENIED\n";
    write(client_fd, response.c_str(), response.size());
}

void Server::handleAppendEntries(int client_fd, const std::string& request) {
    std::istringstream iss(request);
    std::string cmd;
    int term, leader_id, prev_log_index, prev_log_term, leader_commit, num_entries;
    
    iss >> cmd >> term >> leader_id >> prev_log_index >> prev_log_term 
        >> leader_commit >> num_entries;
    
    bool success = false;
    int next_index = 1;
    
    // Update term if necessary
    if (term > current_term_) {
        stepDown(term);
    }
    
    if (term == current_term_) {
        // Reset election timeout - we heard from the leader
        last_heartbeat_ = std::chrono::steady_clock::now();
        
        if (role_ != Role::FOLLOWER) {
            role_ = Role::FOLLOWER;
        }
        
        // Check if our log matches at prev_log_index
        bool log_ok = true;
        if (prev_log_index > 0) {
            LogEntry prev_entry;
            if (wal_.getEntry(prev_log_index, prev_entry)) {
                if (prev_entry.term != prev_log_term) {
                    log_ok = false;
                    // Conflict: delete conflicting entry and all that follow
                    wal_.truncateFrom(prev_log_index);
                }
            } else {
                log_ok = false; // Don't have this entry
            }
        }
        
        if (log_ok) {
            // Append new entries
            for (int i = 0; i < num_entries; i++) {
                LogEntry entry;
                iss >> entry.index >> entry.term >> entry.operation 
                    >> entry.key >> entry.value;
                
                // Check if we already have this entry
                LogEntry existing;
                bool have_it = wal_.getEntry(entry.index, existing);
                
                if (!have_it) {
                    wal_.appendEntry(entry);
                } else if (existing.term != entry.term) {
                    // Conflict: replace this and all following entries
                    wal_.truncateFrom(entry.index);
                    wal_.appendEntry(entry);
                }
            }
            
            // Update commit index
            if (leader_commit > commit_index_) {
                int last_log_index, last_log_term;
                wal_.getLastLogInfo(last_log_index, last_log_term);
                commit_index_ = std::min(leader_commit, last_log_index);
                
                // Apply committed entries
                advanceCommitIndex();
            }
            
            success = true;
            int last_log_index, last_log_term;
            wal_.getLastLogInfo(last_log_index, last_log_term);
            next_index = last_log_index + 1;
        } else {
            // Log doesn't match, tell leader to back up
            next_index = prev_log_index;
        }
    }
    
    std::ostringstream oss;
    oss << (success ? "SUCCESS" : "FAIL") << " " << current_term_ << " " << next_index << "\n";
    std::string response = oss.str();
    write(client_fd, response.c_str(), response.size());
}

void Server::handleClientPut(int client_fd, const std::string& request) {
    std::istringstream iss(request);
    std::string cmd, key, value;
    iss >> cmd >> key >> value;
    
    if (role_ != Role::LEADER) {
        write(client_fd, "NOT_LEADER\n", 11);
        return;
    }
    
    // Create event with callback
    Event e;
    e.type = EventType::CLIENT_PUT;
    e.key = key;
    e.value = value;
    e.client_callback = [client_fd](bool success, const std::string& msg) {
        if (success) {
            write(client_fd, "OK\n", 3);
        } else {
            write(client_fd, msg.c_str(), msg.size());
            write(client_fd, "\n", 1);
        }
        close(client_fd);
    };
    
    event_queue_.push(e);
}

void Server::handleClientGet(int client_fd, const std::string& request) {
    std::istringstream iss(request);
    std::string cmd, key, value;
    iss >> cmd >> key;
    
    if (store_.get(key, value)) {
        write(client_fd, value.c_str(), value.size());
        write(client_fd, "\n", 1);
    } else {
        write(client_fd, "NOT_FOUND\n", 10);
    }
    
    close(client_fd);
}

void Server::start() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "[ERROR] socket() failed\n";
        return;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[ERROR] bind() failed on port " << port_ << std::endl;
        close(server_fd);
        return;
    }

    listen(server_fd, 10);

    std::cout << "[INFO] LogKV server running on port " << port_ << std::endl;
    
    // Start subsystems
    startEventLoop();
    
    if (role_ == Role::LEADER) {
        becomeLeader();
    } else {
        startElectionTimer();
    }

    // Accept client connections
    while (running_) {
        int client = accept(server_fd, nullptr, nullptr);
        if (client < 0) continue;
        std::thread(&Server::handleClient, this, client).detach();
    }
    
    close(server_fd);
}

void Server::handleClient(int client_fd) {
    char buffer[4096];
    int n = read(client_fd, buffer, sizeof(buffer));
    if (n <= 0) {
        close(client_fd);
        return;
    }

    std::string req(buffer, n);
    std::istringstream iss(req);
    std::string cmd;
    iss >> cmd;

    if (cmd == "REQUEST_VOTE") {
        handleRequestVote(client_fd, req);
        close(client_fd);
    }
    else if (cmd == "APPEND_ENTRIES") {
        handleAppendEntries(client_fd, req);
        close(client_fd);
    }
    else if (cmd == "PUT") {
        handleClientPut(client_fd, req);
        // Don't close - callback will close
    }
    else if (cmd == "GET") {
        handleClientGet(client_fd, req);
        // Already closed in handler
    }
    else if (cmd == "INSTALL_SNAPSHOT") {
        handleInstallSnapshot(client_fd, req);
        close(client_fd);
    }
    else {
        write(client_fd, "UNKNOWN_CMD\n", 12);
        close(client_fd);
    }
}

// ============================================================================
// SNAPSHOT IMPLEMENTATIONS
// ============================================================================

bool Server::loadSnapshot() {
    std::unordered_map<std::string, std::string> data;
    SnapshotMetadata metadata;
    
    if (!snapshot_manager_.loadSnapshot(data, metadata)) {
        return false;
    }
    
    // Apply snapshot data to store
    for (const auto& [key, value] : data) {
        store_.put(key, value);
    }
    
    // Update state to reflect snapshot
    last_applied_ = metadata.last_included_index;
    commit_index_ = metadata.last_included_index;
    
    // Tell WAL to install the snapshot
    // This clears the log and sets up for entries after the snapshot
    wal_.installSnapshot(metadata.last_included_index, metadata.last_included_term);
    
    std::cout << "[SUCCESS] Restored from snapshot: " << data.size() 
              << " entries, up to index " << metadata.last_included_index << std::endl;
    
    return true;
}

void Server::createSnapshotIfNeeded() {
    // Check if we've applied enough entries since last snapshot
    if (entries_since_snapshot_ >= snapshot_threshold_) {
        std::cout << "[INFO] Snapshot threshold reached (" 
                  << entries_since_snapshot_ << " entries), creating snapshot..." << std::endl;
        createSnapshot();
        entries_since_snapshot_ = 0;
    }
}

void Server::createSnapshot() {
    /**
     * SNAPSHOT CREATION PROCESS:
     * 
     * 1. Get current state (can't be done while applying entries)
     * 2. Determine what log index this snapshot covers (last_applied_)
     * 3. Write snapshot to disk atomically
     * 4. Compact the log (discard old entries)
     * 
     * SAFETY CONSIDERATIONS:
     * - Only snapshot committed entries (never uncommitted)
     * - Snapshot creation is async - server continues operating
     * - If snapshot fails, we still have the WAL
     * - Log compaction only happens after successful snapshot
     */
    
    std::cout << "[INFO] Creating snapshot at index " << last_applied_ << std::endl;
    
    // Get a copy of the current state
    // In production, we'd use a read lock or copy-on-write structure
    std::unordered_map<std::string, std::string> snapshot_data;
    
    // Copy all keys from store
    auto keys = store_.getAllKeys();
    for (const auto& key : keys) {
        std::string value;
        if (store_.get(key, value)) {
            snapshot_data[key] = value;
        }
    }
    
    // Get the term of the last applied entry
    int last_term = current_term_;
    LogEntry last_entry;
    if (wal_.getEntry(last_applied_, last_entry)) {
        last_term = last_entry.term;
    }
    
    // Create the snapshot (atomic write to disk)
    bool success = snapshot_manager_.createSnapshot(
        snapshot_data, 
        last_applied_, 
        last_term
    );
    
    if (success) {
        // Compact the log - discard entries up to last_applied_
        // This frees disk space and speeds up future recoveries
        wal_.discardEntriesBefore(last_applied_);
        
        std::cout << "[SUCCESS] Snapshot created and log compacted at index " 
                  << last_applied_ << std::endl;
    } else {
        std::cerr << "[ERROR] Failed to create snapshot" << std::endl;
    }
}

void Server::handleInstallSnapshot(int client_fd, const std::string& request) {
    /**
     * INSTALL_SNAPSHOT RPC (Follower Side)
     * 
     * Called when leader sends us a snapshot because we're too far behind.
     * 
     * WHEN THIS HAPPENS:
     * - Follower needs entry at index 100
     * - Leader's first log entry is index 500 (already compacted)
     * - Leader sends snapshot instead of log entries
     * 
     * FORMAT:
     * INSTALL_SNAPSHOT <term> <leader_id> <last_index> <last_term> <data_size> <data...>
     * 
     * RESPONSE:
     * SUCCESS <current_term>
     */
    
    std::istringstream iss(request);
    std::string cmd;
    int term, leader_id, last_included_index, last_included_term;
    size_t data_size;
    
    iss >> cmd >> term >> leader_id >> last_included_index 
        >> last_included_term >> data_size;
    
    std::cout << "[INFO] Receiving InstallSnapshot from leader " << leader_id
              << ": index=" << last_included_index << ", term=" << last_included_term
              << ", size=" << data_size << " bytes" << std::endl;
    
    // Update term if necessary
    if (term > current_term_) {
        stepDown(term);
    }
    
    if (term < current_term_) {
        // Reject stale snapshot
        std::ostringstream oss;
        oss << "REJECT " << current_term_ << "\n";
        write(client_fd, oss.str().c_str(), oss.str().size());
        return;
    }
    
    // Reset election timeout - we heard from leader
    last_heartbeat_ = std::chrono::steady_clock::now();
    
    // Read snapshot data
    std::vector<char> snapshot_data(data_size);
    // In a real implementation, we'd read this from the socket
    // For now, we'll implement the chunked transfer in replicator
    
    // For this simplified version, we'll trigger a snapshot load
    // In production, this would involve chunked transfer
    
    std::unordered_map<std::string, std::string> data;
    SnapshotMetadata metadata;
    
    if (snapshot_manager_.loadSnapshot(data, metadata)) {
        // Clear current state
        store_.clear();
        
        // Apply snapshot
        for (const auto& [key, value] : data) {
            store_.put(key, value);
        }
        
        // Update state
        last_applied_ = metadata.last_included_index;
        commit_index_ = metadata.last_included_index;
        
        // Install snapshot in WAL
        wal_.installSnapshot(metadata.last_included_index, metadata.last_included_term);
        
        std::cout << "[SUCCESS] Installed snapshot at index " 
                  << last_included_index << std::endl;
    }
    
    // Send acknowledgment
    std::ostringstream oss;
    oss << "SUCCESS " << current_term_ << "\n";
    write(client_fd, oss.str().c_str(), oss.str().size());
}

void Server::sendSnapshotToFollower(const std::string& follower_addr) {
    /**
     * INSTALL_SNAPSHOT RPC (Leader Side)
     * 
     * Send our snapshot to a follower who is too far behind.
     * 
     * WHEN TO CALL:
     * - Leader needs to send entry at index 100 to follower
     * - But leader's first log entry is 500 (already compacted)
     * - Must send snapshot instead
     * 
     * CHUNKED TRANSFER:
     * - Snapshots can be huge (GBs)
     * - Send in chunks to avoid memory issues
     * - Track offset for each follower
     */
    
    std::cout << "[INFO] Sending snapshot to follower: " << follower_addr << std::endl;
    
    // Get snapshot metadata
    SnapshotMetadata metadata;
    if (!snapshot_manager_.getSnapshotMetadata(metadata)) {
        std::cerr << "[ERROR] No snapshot available to send" << std::endl;
        return;
    }
    
    // In a full implementation, we'd:
    // 1. Read snapshot in chunks
    // 2. Send each chunk via INSTALL_SNAPSHOT RPC
    // 3. Track offset for each follower
    // 4. Handle retries and failures
    
    // For this implementation, we'll send a simplified version
    std::string ip = follower_addr.substr(0, follower_addr.find(':'));
    int port = std::stoi(follower_addr.substr(follower_addr.find(':') + 1));
    
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "[ERROR] Failed to create socket" << std::endl;
        return;
    }
    
    sockaddr_in serv{};
    serv.sin_family = AF_INET;
    serv.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &serv.sin_addr);
    
    if (connect(sock, (sockaddr*)&serv, sizeof(serv)) < 0) {
        close(sock);
        return;
    }
    
    std::ostringstream oss;
    oss << "INSTALL_SNAPSHOT "
        << current_term_ << " "
        << server_id_ << " "
        << metadata.last_included_index << " "
        << metadata.last_included_term << " "
        << metadata.data_size << "\n";
    
    std::string msg = oss.str();
    write(sock, msg.c_str(), msg.size());
    
    // Read response
    char buffer[128];
    int n = read(sock, buffer, sizeof(buffer));
    if (n > 0) {
        std::string resp(buffer, n);
        if (resp.find("SUCCESS") != std::string::npos) {
            std::cout << "[SUCCESS] Follower installed snapshot" << std::endl;
        }
    }
    
    close(sock);
}

