#include <iostream>
#include <sstream>
#include <string>

#include "kv_store.h"
#include "wal.h"

int main() {
    KVStore store;
    WriteAheadLog wal("store.wal");

    wal.replay(store);

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string command;
        iss >> command;

        if (command == "PUT") {
            std::string key, value;
            iss >> key >> value;

            if (key.empty() || value.empty()) {
                std::cout << "ERROR\n";
                continue;
            }

            wal.append("PUT " + key + " " + value);
            store.put(key, value);

            std::cout << "OK\n";
        }
        else if (command == "GET") {
            std::string key;
            iss >> key;

            auto result = store.get(key);
            if (result.has_value()) {
                std::cout << result.value() << "\n";
            } else {
                std::cout << "NOT_FOUND\n";
            }
        }
        else if (command == "EXIT") {
            break;
        }
        else {
            std::cout << "UNKNOWN_COMMAND\n";
        }
    }

    return 0;
}
