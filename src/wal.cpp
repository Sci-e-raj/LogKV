#include "wal.h"
#include <sstream>

WriteAheadLog::WriteAheadLog(const std::string& filename)
    : filename_(filename) {}

void WriteAheadLog::appendPut(int index,
                             const std::string& key,
                             const std::string& value) {
    std::ofstream out(filename_, std::ios::app);
    out << index << " PUT " << key << " " << value << "\n";
    out.flush();
}


void WriteAheadLog::replay(KVStore& store) {
    std::ifstream in(filename_);
    std::string line;

    while (std::getline(in, line)) {
        std::istringstream iss(line);
        int index;
        std::string cmd, key, value;
        iss >>index>>cmd >> key >> value;
        if (cmd == "PUT") {
            store.put(key, value);
        }
    }
}
