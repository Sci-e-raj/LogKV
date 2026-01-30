#pragma once
#include <unordered_map>
#include <string>
#include <mutex>

class KVStore {
public:
    void put(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string& value);

private:
    std::unordered_map<std::string, std::string> data_;
    std::mutex mutex_;
};