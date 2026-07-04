CXX = g++
CC = gcc
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
  $(shell if not exist "$(subst /,\,$(OBJ_DIR)/$(SRC_DIR))" mkdir "$(subst /,\,$(OBJ_DIR)/$(SRC_DIR))")
  $(shell if not exist "$(subst /,\,$(OBJ_DIR)/$(MINIUPNP_DIR))" mkdir "$(subst /,\,$(OBJ_DIR)/$(MINIUPNP_DIR))")
  $(shell if not exist "$(subst /,\,$(BUILD_DIR))" mkdir "$(subst /,\,$(BUILD_DIR))")
  MKDIR = @if not exist "$(subst /,\,$(@D))" mkdir "$(subst /,\,$(@D))"
else
  TARGET = $(BUILD_DIR)/ezsock
  LIBS = -lpthread
  $(shell mkdir -p $(OBJ_DIR)/$(SRC_DIR) $(OBJ_DIR)/$(MINIUPNP_DIR) $(BUILD_DIR))
  MKDIR = @mkdir -p $(@D)
endif

all: $(TARGET)

$(TARGET): $(CXX_OBJ) $(MINIUPNP_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

$(OBJ_DIR)/$(SRC_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(MKDIR)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJ_DIR)/$(MINIUPNP_DIR)/%.o: $(MINIUPNP_DIR)/%.c
	$(MKDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

ifneq ($(OS),Windows_NT)
clean:
	rm -rf $(BUILD_DIR)
else
clean:
	-if exist "$(BUILD_DIR)" rmdir /s /q "$(BUILD_DIR)"
endif

.PHONY: all clean
