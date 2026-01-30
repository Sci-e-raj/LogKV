#pragma once

#include <string>
#include <unordered_map>
#include <optional>

class KVStore {
public:
    void put(const std::string& key, const std::string& value);

    std::optional<std::string> get(const std::string& key) const;

private:
    std::unordered_map<std::string, std::string> store_;
};
