#pragma once
#include "store.h"
#include "wal.h"
#include "replication.h"
#include <memory>

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
    int port_;
    Role role_;
    
    KVStore store_;
    WriteAheadLog wal_;

    void handleClient(int client_fd);
};