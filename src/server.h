#pragma once
#include "store.h"
#include "wal.h"
#include "replication.h"
#include "snapshot.h"
#include <memory>
#include <atomic>
#include <chrono>
#include <random>
#include "event_queue.h"
#include <thread>
#include <unordered_map>

enum class Role {
    LEADER,
    FOLLOWER,
    CANDIDATE
};

struct PendingClientRequest {
    int log_index;
    std::function<void(bool, const std::string&)> callback;
};

class Server {
public:
    Server(int port, Role role, int server_id,
           const std::vector<std::string>& peers);
    
    ~Server();

    void start();
    void shutdown();

private:
    // Core Raft state (persistent)
    int current_term_ = 0;
    int voted_for_ = -1;
    
    // Volatile state
    int commit_index_ = 0;
    int last_applied_ = 0;
    
    // Leader state
    std::unique_ptr<Replicator> replicator_;
    
    // Server configuration
    int port_;
    int server_id_;
    std::vector<std::string> peers_;
    std::atomic<Role> role_;
    
    // Storage
    KVStore store_;
    WriteAheadLog wal_;
    SnapshotManager snapshot_manager_;
    
    // Snapshot configuration
    int snapshot_threshold_ = 1000;  // Take snapshot every N log entries
    std::atomic<int> entries_since_snapshot_{0};
    
    // Event-driven architecture
    EventQueue event_queue_;
    std::thread event_loop_thread_;
    
    // Timing
    std::chrono::steady_clock::time_point last_heartbeat_;
    std::mt19937 rng_;
    
    // Client request tracking
    std::unordered_map<int, PendingClientRequest> pending_requests_;
    std::mutex pending_requests_mutex_;
    
    // Thread management
    std::atomic<bool> running_{true};
    std::thread heartbeat_thread_;
    std::thread election_thread_;
    
    // Core event loop
    void startEventLoop();
    void processEvent(const Event& e);
    
    // Raft RPCs - Server side
    void handleAppendEntries(int client_fd, const std::string& request);
    void handleRequestVote(int client_fd, const std::string& request);
    
    // Client operations
    void handleClientPut(int client_fd, const std::string& request);
    void handleClientGet(int client_fd, const std::string& request);
    
    // Election and heartbeat management
    void startElection();
    void becomeLeader();
    void stepDown(int new_term);
    void startHeartbeatSender();
    void startElectionTimer();
    int getElectionTimeout();
    
    // Log management
    void applyLogEntry(const LogEntry& entry);
    void advanceCommitIndex();
    
    // Snapshot management
    void createSnapshotIfNeeded();
    void createSnapshot();
    bool loadSnapshot();
    void handleInstallSnapshot(int client_fd, const std::string& request);
    void sendSnapshotToFollower(const std::string& follower_addr);
    
    // Network
    void handleClient(int client_fd);
    
    // Persistence
    void persistState();
    void loadState();
};
