#include "server.h"

int main(int argc, char* argv[]) {
    int port = std::stoi(argv[1]);
    Server server(port, Role::FOLLOWER);
    server.start();
    return 0;
}
