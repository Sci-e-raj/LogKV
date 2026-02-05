#include "store.h"
#include <algorithm>

void KVStore::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_[key] = value;
}

bool KVStore::get(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end()) return false;
    value = it->second;
    return true;
}

bool KVStore::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.erase(key) > 0;
}

bool KVStore::exists(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.find(key) != data_.end();
}

std::vector<std::string> KVStore::getAllKeys() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> keys;
    keys.reserve(data_.size());
    
    for (const auto& pair : data_) {
        keys.push_back(pair.first);
    }
    
    std::sort(keys.begin(), keys.end());
    return keys;
}

size_t KVStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.size();
}

void KVStore::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    data_.clear();
}