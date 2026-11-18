cmake_minimum_required(VERSION 3.14)
include(FetchContent)

project(sod
    LANGUAGES CXX
    VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(sod
    src/main.cpp
    src/sendler.cpp
    src/receiver.cpp
    )

target_include_directories(sod PRIVATE include)



