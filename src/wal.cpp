#include "wal.h"
#include "kv_store.h"

#include <fstream>
#include <sstream>

WriteAheadLog::WriteAheadLog(const std::string& filename)
    : filename_(filename), log_file_(filename, std::ios::app) {}

void WriteAheadLog::append(const std::string& entry) {
    log_file_ << entry << "\n";
    log_file_.flush();  // ensures data is written
}

void WriteAheadLog::replay(KVStore& store) {
    std::ifstream in(filename_);
    std::string line;

    while (std::getline(in, line)) {
        std::istringstream iss(line);
        std::string command;
        iss >> command;

        if (command == "PUT") {
            std::string key, value;
            iss >> key >> value;
            if (!key.empty() && !value.empty()) {
                store.put(key, value);
            }
        }
    }
}
