#pragma once

#include <string>
#include <fstream>

class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::string& filename);

    void append(const std::string& entry);

    void replay(class KVStore& store);

private:
    std::string filename_;
    std::ofstream log_file_;
};
