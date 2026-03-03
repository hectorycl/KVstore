# 1. 编译器与选项
CC = gcc
CXX = g++
# 包含所有必要的头文件路径
CFLAGS = -Wall -g -pthread -I./include -I./include/index 
CXXFLAGS = -Wall -g -pthread -I./include -I./include/index

# 2. 目录定义
SRC_DIR = src
INDEX_DIR = src/index
INCLUDE_DIR = include
TEST_DIR = test

# 3. 源文件与对象文件映射
# 逻辑：KVStore 由 bptree.c 和 kvstore.c 组成底层，test_kvstore.c 负责调用测试
C_FILES = $(INDEX_DIR)/bptree.c $(SRC_DIR)/kvstore.c
TEST_FILES = $(TEST_DIR)/test_kvstore.c

# 将 .c 文件名替换为 .o 文件名
OBJ_FILES = $(INDEX_DIR)/bptree.o $(SRC_DIR)/kvstore.o $(TEST_DIR)/test_kvstore.o

# 4. 目标程序
TARGET = test_kvstore

# --- 编译规则 ---

all: $(TARGET)

# 链接阶段：注意 $@ 和 $^ 之间的空格，确保不会粘连
$(TARGET): $(OBJ_FILES)
	$(CXX) $(CXXFLAGS) -o $@ $^

# 编译 src/index 目录下的 C 文件
$(INDEX_DIR)/%.o: $(INDEX_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# 编译 src 目录下的 C 文件
$(SRC_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# 编译 test 目录下的测试文件
# 即使是 .c 文件，如果用了 C++ 的东西（如 cin/cout），也建议用 CXX 编译
$(TEST_DIR)/%.o: $(TEST_DIR)/%.c
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 清理
clean:
	rm -f $(OBJ_FILES) $(TARGET)

.PHONY: all clean