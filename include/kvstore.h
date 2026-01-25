// /home/ubuntu/c++2512/KVstore/include/kvstore.h

/**
 * kvstore 结构体负责整个存储引擎，包含一个指向 bptree 的指针
 * B+ 树则是用来存储键值对的数据结构
 */

#ifndef KVSTORE_H
#define KVSTORE_H


#include <stdio.h>
#include "index/bptree.h"

#ifdef __cplusplus
extern "C" {
#endif

// 定义 KVSTORE 的结构体
typedef struct _kvstore {
    bptree* tree;  // B+ 树指针

    FILE* log_fp;  // 日志文件指针
    char log_path[256];
} kvstore;

// =============  kvstore 基本操作函数  =============
// 创建 kvstore
kvstore* kvstore_create(const char* log_path);
void kvstore_destroy(kvstore* store);

// KV 操作
int kvstore_insert(kvstore* store, int key, long value);
int kvstore_search(kvstore* store, int key, long* out_value);
int kvstore_delete(kvstore* store, int key);



#ifdef __cplusplus
}
#endif

#endif