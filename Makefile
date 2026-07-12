CXX = clang++
CC = clang
CXXFLAGS = -std=c++23 -Iinclude -Iminiupnp -Wall -Wextra -O2
CFLAGS = -Iinclude -Iminiupnp -Wall -Wextra -O2

SRC_DIR = src
MINIUPNP_DIR = miniupnp
BUILD_DIR = build
OBJ_DIR = $(BUILD_DIR)/obj

SRC = $(wildcard $(SRC_DIR)/*.cpp)
MINIUPNP_SRC = $(wildcard $(MINIUPNP_DIR)/*.c)

CXX_OBJ = $(patsubst $(SRC_DIR)/%.cpp,$(OBJ_DIR)/$(SRC_DIR)/%.o,$(SRC))
MINIUPNP_OBJ = $(patsubst $(MINIUPNP_DIR)/%.c,$(OBJ_DIR)/$(MINIUPNP_DIR)/%.o,$(MINIUPNP_SRC))

ifeq ($(OS),Windows_NT)
  TARGET = $(BUILD_DIR)/ezsock.exe
  LIBS = -lws2_32
else
  TARGET = $(BUILD_DIR)/ezsock
  LIBS = -lpthread
endif

all: $(TARGET)

$(TARGET): $(CXX_OBJ) $(MINIUPNP_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

$(OBJ_DIR)/$(SRC_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJ_DIR)/$(MINIUPNP_DIR)/%.o: $(MINIUPNP_DIR)/%.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all clean compiledb
