#pragma once
#include <unordered_map>
#include <string>
#include <mutex>
#include <vector>

class KVStore {
public:
    void put(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string& value);
    bool remove(const std::string& key);
    bool exists(const std::string& key);
    
    // Get all keys (for debugging/admin)
    std::vector<std::string> getAllKeys();
    
    // Get store size
    size_t size() const;
    
    // Clear all data (for testing)
    void clear();

private:
    std::unordered_map<std::string, std::string> data_;
    mutable std::mutex mutex_;
};