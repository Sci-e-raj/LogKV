#include "server.h"
#include <arpa/inet.h>
#include <unistd.h>
#include <sstream>
#include <thread>
#include <iostream>

void Server::startHeartbeatMonitor() {
    last_heartbeat_ = std::chrono::steady_clock::now();

    std::thread([this]() {
        while (true) {
            auto now = std::chrono::steady_clock::now();
            auto diff = std::chrono::duration_cast<std::chrono::seconds>(
                now - last_heartbeat_
            ).count();

            if (diff > 3) {
                leader_alive_ = false;
                std::cout << "[WARN] Leader considered dead\n";
            }

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }).detach();
}

void Server::startHeartbeatSender() {
    std::thread([this]() {
        while (true) {
            if (replicator_) {
                replicator_->sendHeartbeats();
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }).detach();
}

Server::Server(int port, Role role)
    : port_(port), role_(role), wal_("wal.log") {

    wal_.replay(store_);

    if (role_ == Role::LEADER) {
        replicator_ = std::make_unique<Replicator>(
            std::vector<std::string>{
                "127.0.0.1:8081",
                "127.0.0.1:8082"
            }
        );
    }
}


void Server::start() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    bind(server_fd, (sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 10);

    std::cout << "LogKV running on port " << port_ << std::endl;

    if (role_ == Role::LEADER) {
        startHeartbeatSender();
    }
    if (role_ == Role::FOLLOWER) {
        startHeartbeatMonitor();
    }

    while (true) {
        int client = accept(server_fd, nullptr, nullptr);
        std::thread(&Server::handleClient, this, client).detach();
    }
}

void Server::handleClient(int client_fd) {
    char buffer[1024];
    int n = read(client_fd, buffer, sizeof(buffer));
    if (n <= 0) {
        close(client_fd);
        return;
    }

    std::string req(buffer, n);
    std::istringstream iss(req);

    std::string cmd;
    iss >> cmd;

    if (cmd == "HEARTBEAT") {
        last_heartbeat_ = std::chrono::steady_clock::now();
        leader_alive_ = true;

        write(client_fd, "OK\n", 3);
        close(client_fd);
        return;
    }

    // ---------- REPLICATION MESSAGE ----------
    if (cmd == "REPL_PUT") {
        std::string key, value;
        iss >> key >> value;

        wal_.appendPut(key, value);
        store_.put(key, value);

        write(client_fd, "ACK\n", 4);
        close(client_fd);
        return;
    }

    // ---------- CLIENT PUT ----------
    if (cmd == "PUT") {
        std::string key, value;
        iss >> key >> value;

        if (role_ == Role::FOLLOWER) {
            write(client_fd, "NOT_LEADER\n", 11);
            close(client_fd);
            return;
        }

        wal_.appendPut(key, value);
        store_.put(key, value);

        if (replicator_) {
            replicator_->replicatePut(key, value);
        }

        write(client_fd, "OK\n", 3);
        close(client_fd);
        return;
    }

    // ---------- CLIENT GET ----------
    if (cmd == "GET") {
        std::string key, value;
        iss >> key;

        if (store_.get(key, value)) {
            write(client_fd, value.c_str(), value.size());
            write(client_fd, "\n", 1);
        } else {
            write(client_fd, "NOT_FOUND\n", 10);
        }

        close(client_fd);
        return;
    }

    // ---------- UNKNOWN ----------
    write(client_fd, "UNKNOWN_CMD\n", 12);
    close(client_fd);
}
