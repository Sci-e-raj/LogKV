cmake_minimum_required(VERSION 3.10)
project(logkv)

set(CMAKE_CXX_STANDARD 17)

add_executable(logkv
    src/main.cpp
    src/server.cpp
    src/wal.cpp
    src/store.cpp
    src/replication.cpp
)
