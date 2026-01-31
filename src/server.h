#pragma once
#include "store.h"
#include "wal.h"
#include "replication.h"
#include <memory>
#include <atomic>
#include <chrono>

enum class Role {
    LEADER,
    FOLLOWER
};

class Server {
public:
    Server(int port, Role role);
    void start();
    std::unique_ptr<Replicator> replicator_;
   
private:
    std::atomic<bool> leader_alive_{true};
    std::chrono::steady_clock::time_point last_heartbeat_;
    void startHeartbeatSender();
    void startHeartbeatMonitor();

    int port_;
    Role role_;
    
    KVStore store_;
    WriteAheadLog wal_;

    void handleClient(int client_fd);
};