#include "kv_store.h"

void KVStore::put(const std::string& key, const std::string& value) {
    store_[key] = value;
}

std::optional<std::string> KVStore::get(const std::string& key) const {
    auto it = store_.find(key);
    if (it == store_.end()) {
        return std::nullopt;
    }
    return it->second;
}
