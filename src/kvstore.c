#include "kvstore.h"

#include <stdio.h>
#include <stdlib.h>

static int kvstore_open_log(kvstore* store, const char* path);
static int kvstore_replay_log(kvstore* store);
static int kvstore_log_put(kvstore* store, int key, long value);
static int kvstore_log_del(kvstore* store, int key);

// 创建 KVstore
kvstore* kvstore_create(const char* log_path) {
    kvstore* store = (kvstore*)malloc(sizeof(kvstore));  // 分配内存
    if (!store) {
        perror("malloc kvstore");
        exit(EXIT_FAILURE);
    }

    store->tree = bptree_create();
    store->log_fp = NULL;

    if (kvstore_open_log(store, log_path) != 0) {
        free(store);
        return NULL;
    }

    // 关键：恢复数据
    // 数据回复发生在 B+ 树创建之后，对用户透明
    kvstore_replay_log(store);

    return store;
}

/**
 * 销毁 kvstore (清理函数)
 *
 *  *  - 如果只 free(store), store 指针没有了，B+ 树所有节点没有被释放，变成了“无法访问的垃圾” -> 内存泄漏 ！
 *  - bptree_destroy 递归 free B+ 树的每个节点，最后再把 store 结构体占用的内存释放掉
 *
 * 代码层面的理解：
 *  - 创建一个对象时是由外向内：
 *      sotre = malloc(...)
 *      sotre->tree = bptree_create()
 *  - 销毁时必须严格反序（由外向内）：
 *      bptree_destroy(...)
 *      free(store)
 *
 *
 * fclose 作用：
 *      - 刷盘（Flush）:写日志时，系统为了效率，通常会将数据放在缓冲区。调用 fclose
 *                      会强迫系统把缓冲区里剩下的数据全部写进硬盘，否则丢失几条记录
 *      - 释放句柄：操作系统对一个程序能同时打开的文件数量是有上限的。
 *                  如果不关闭，文件就会被一直锁定，其他程序（甚至是下次启动的程序）可能无法再次访问它。
 *
 *
 */
void kvstore_destroy(kvstore* store) {
    if (!store) return;

    if (store->log_fp) {
        fclose(store->log_fp);
    }

    bptree_destroy(store->tree);
    free(store);

    return;
}

// 插入数据到 KVstore
int kvstore_insert(kvstore* store, int key, long value) {
    int ret = bptree_insert(store->tree, key, value);
    if (ret == 0) {
        // 操作成功，记录日志
        kvstore_log_put(store, key, value);
    }

    return ret;
}

// 查找数据
int kvstore_search(kvstore* store, int key, long* value) {
    return bptree_search(store->tree, key, value);
}

// 删除数据
int kvstore_delete(kvstore* store, int key) {
    int ret = bptree_delete(store->tree, key);
    if (ret == 0) {
        kvstore_log_del(store, key);
    }
    return ret;
}

/**
 * 打开 / 创建日志文件
 * static - 函数仅当前文件（.c）可见，是一个私有方法，test_kvstore.c 无法直接调用
 * const - 保证函数内部不会直接修改这个路径字符串
 * snprintf - 把传入的路径字符串 path 复制到 store 结构体的 log_path 字符数组中
 *          - 会参考 store->log_path ，确保最多只拷贝数组能容纳的长度！
 * fopen - 系统调用，去硬盘上找这个文件
 * a+   - 追加/读写模式，持久化的关键
 *      - Append(a) -> 文件已存在，光标直接跳到文件末尾   ---- 追加
 *      - Update(+) -> 允许既能写也能读
 *      - Create    -> 如果文件不存在，自动创建一个新文件
 *
 * perror - 自动打印失败的具体原因
 *  */
static int kvstore_open_log(kvstore* store, const char* path) {
    snprintf(store->log_path, sizeof(store->log_path), "%s", path);

    store->log_fp = fopen(path, "a+");
    if (!store->log_fp) {
        perror("fopen log file");
        return -1;
    }

    return 0;
}

/**
 * 日志重放（replay）
 *
 * rewind - 把文件的读取+指针（光标） 重置到文件开头（a+ 模式打开， 默认在文件末尾）
 * fgets  - 从文件中读取一行，直到换行符 \n 或者读满了255个字符
 * sscanf - 从字符串里提取数据吗，返回成功汽配并复制的变量个数
 *          scanf(...): 从键盘输入（标准输入）读取。
 *          fscanf(fp, ...): 直接从文件读取。
 *          sscanf(str, ...): 从内存中的字符串读取
 *
 *
 */
static int kvstore_replay_log(kvstore* store) {
    char line[256];

    rewind(store->log_fp);

    while (fgets(line, sizeof(line), store->log_fp)) {
        int key;
        long value;

        if (sscanf(line, "PUT %d %ld", &key, &value) == 2) {
            bptree_insert(store->tree, key, value);
        } else if (sscanf(line, "DEL %d", &key) == 1) {
            bptree_delete(store->tree, key);
        } else {
            // 忽略格式错误的行
            continue;
        }
    }

    return 0;
}

/**
 * 写日志函数
 *
 */
static int kvstore_log_put(kvstore* store, int key, long value) {
    if (!store->log_fp) return -1;

    fprintf(store->log_fp, "PUT %d %ld\n", key, value);
    fflush(store->log_fp);

    return 0;
}

static int kvstore_log_del(kvstore* store, int key) {
    if (!store->log_fp) return -1;

    fprintf(store->log_fp, "DEL %d\n", key);
    fflush(store->log_fp);

    return 0;
}
