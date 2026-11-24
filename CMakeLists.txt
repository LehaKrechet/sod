cmake_minimum_required(VERSION 3.16)
project(sod_multicast LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_executable(sod
    src/main.cpp
    src/config.cpp
    src/multicast.cpp
    src/discovery.cpp
)
target_include_directories(sod PRIVATE src)
target_compile_options(sod PRIVATE -O2 -Wall -Wextra -Wpedantic)
target_link_libraries(sod PRIVATE pthread)
