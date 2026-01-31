#pragma once
#include <string>
#include <vector>

class Replicator {
public:
    Replicator(const std::vector<std::string>& followers);
    bool replicatePut(int index,
                  const std::string& key,
                  const std::string& value);

    void sendHeartbeats();
    const std::vector<std::string>& followers() const;

private:
    std::vector<std::string> followers_;
};
