// /home/ubuntu/c++2512/KVstore/include/kvstore.h

/**
 * kvstore 结构体负责整个存储引擎，包含一个指向 bptree 的指针
 * B+ 树则是用来存储键值对的数据结构
 */

#ifndef KVSTORE_H
#define KVSTORE_H

#include <stdio.h>

#include "index/bptree.h"

#define KVSTORE_MAX_OPS 1000
#define KVSTORE_MAX_LOG_SIZE 4 * 1024 * 1024  // 4MB

// kvstore error codes
#define KVSTORE_OK 0

// 通用错误（-1 ~ -9）
#define KVSTORE_ERR_NULL   -1    // 空指针 / 非法参数
#define KVSTORE_ERR_INTERNAL -2  // 内部错误（不应该发生）
#define KVSTORE_ERR_IO       -3  // 文件 / IO 错误

// 状态相关错误（-10 ~ -19）
#define KVSTORE_ERR_READONLY  -10 // 只读模式，禁止写
#define KVSTORE_ERR_RECOVERY  -11 // 恢复中（预留）

// 数据相关错误（-20 ~ -29）
#define KVSTORE_ERR_NOT_FOUND  -20 // key 不存在
#define KVSTORE_ERR_CORRUPTED  -21 // 数据损坏（crc / 格式）

// 空间 / 资源错误（-30 ~ -39）
// #define KVSTORE_ERR_NO_MENORY  -30
// #define KVSTORE_ERR_NO_SPACE   -31




#ifdef __cplusplus
extern "C" {
#endif

// 定义 KVSTORE 的结构体
typedef struct _kvstore {
    bptree* tree;  // B+ 树指针

    FILE* log_fp;  // 日志文件指针
    char log_path[256];

    size_t log_size;   // 当前日志大小（字节）
    size_t ops_count;  // 自上次 compaction 以来的操作次数

    /*  0：正常模式
        1：只读模式（replay 中）
    */
    int readonly;  // 是否处于只读模式（replay / recovery）
} kvstore;

// =============  kvstore 基本操作函数  =============
// 创建 kvstore
kvstore* kvstore_create(const char* log_path);
void kvstore_destroy(kvstore* store);

// KV 操作
int kvstore_insert(kvstore* store, int key, long value);
int kvstore_search(kvstore* store, int key, long* out_value);
int kvstore_delete(kvstore* store, int key);
int kvstore_compact(kvstore* store);

#ifdef __cplusplus
}
#endif

#endif