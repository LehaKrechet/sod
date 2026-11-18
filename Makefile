CXX ?= g++
CXXFLAGS ?= -std=c++17 -O3 -march=native -Wall -Wextra -Wpedantic -pthread
LDFLAGS ?= -pthread

all: sod
sod: src/main.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
clean:
	rm -f sod
.PHONY: all clean
