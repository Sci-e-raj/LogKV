#pragma once
#include <string>
#include <vector>

class Replicator {
public:
    Replicator(const std::vector<std::string>& followers);
    void replicatePut(const std::string& key, const std::string& value);

private:
    std::vector<std::string> followers_;
};
