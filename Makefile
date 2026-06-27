CXX = g++
CXXFLAGS = -std=c++23 -Iinclude -Wall -Wextra -O2
SRC = $(wildcard src/*.cpp)
TARGET = simpleIRC

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(TARGET)

.PHONY: all clean