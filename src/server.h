#pragma once
#include "store.h"
#include "wal.h"
#include "replication.h"
#include <memory>
#include <atomic>
#include <chrono>

enum class Role {
    LEADER,
    FOLLOWER,
    CANDIDATE
};

class Server {
public:
    Server(int port, Role role, int server_id,
       const std::vector<std::string>& peers);

    void start();
    std::unique_ptr<Replicator> replicator_;
    void startElection();
    std::vector<std::string> peers_;
    int last_log_index_ = 0;
    int commit_index_ = 0;

private:
    std::atomic<bool> leader_alive_{true};
    std::chrono::steady_clock::time_point last_heartbeat_;
    void startHeartbeatSender();
    void startHeartbeatMonitor();

    void stepDown(int new_term);

    int current_term_ = 0;
    int voted_for_ = -1;
    int server_id_;   // unique per node

    int port_;
    Role role_;
    
    KVStore store_;
    WriteAheadLog wal_;

    void handleClient(int client_fd);
};