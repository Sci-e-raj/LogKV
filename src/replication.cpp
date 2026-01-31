#include "replication.h"
#include <arpa/inet.h>
#include <unistd.h>
#include <sstream>

Replicator::Replicator(const std::vector<std::string>& followers)
    : followers_(followers) {}

const std::vector<std::string>& Replicator::followers() const {
    return followers_;
}

void Replicator::sendHeartbeats() {
    for (const auto& addr : followers_) {
        std::string ip = addr.substr(0, addr.find(':'));
        int port = std::stoi(addr.substr(addr.find(':') + 1));

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) continue;

        sockaddr_in serv{};
        serv.sin_family = AF_INET;
        serv.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &serv.sin_addr);

        if (connect(sock, (sockaddr*)&serv, sizeof(serv)) < 0) {
            close(sock);
            continue;
        }

        std::string msg = "HEARTBEAT\n";

        write(sock, msg.c_str(), msg.size());
        close(sock);
    }
}

bool Replicator::replicatePut(int index,
                             const std::string& key,
                             const std::string& value) {
    int acks = 1; // leader itself

    for (const auto& addr : followers_) {
        std::string ip = addr.substr(0, addr.find(':'));
        int port = std::stoi(addr.substr(addr.find(':') + 1));

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) continue;

        sockaddr_in serv{};
        serv.sin_family = AF_INET;
        serv.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &serv.sin_addr);

        if (connect(sock, (sockaddr*)&serv, sizeof(serv)) < 0) {
            close(sock);
            continue;
        }

        std::ostringstream oss;
        oss << "REPL_PUT "
            << index << " "
            << key << " "
            << value << "\n";

        std::string msg = oss.str();   // ✅ FIX

        write(sock, msg.c_str(), msg.size());

        char buffer[64];
        int n = read(sock, buffer, sizeof(buffer));
        if (n > 0 && std::string(buffer, n).find("ACK") != std::string::npos) {
            acks++;
        }

        close(sock);
    }

    return acks > (followers_.size() + 1) / 2;
}

