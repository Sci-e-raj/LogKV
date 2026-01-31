#include "server.h"

int main() {
    Server server(8080, Role::LEADER);
    server.start();
    return 0;
}