#pragma once
#include <string>
#include <fstream>
#include "store.h"

class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::string& filename);
    void appendPut(int index,
               const std::string& key,
               const std::string& value);

    void replay(KVStore& store);

private:
    std::string filename_;
};