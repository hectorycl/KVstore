#include "kvstore.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// ======== 内部结构体的定义 =======
struct _kvstore {
    bptree* tree;  // B+ 树指针

    FILE* log_fp;  // 日志文件指针
    char log_path[256];

    kvstore_mode_t mode;

    size_t log_size;   // 当前日志大小（字节）
    size_t ops_count;  // 自上次 compaction 以来的操作次数

    /*  0：正常模式
        1：只读模式（replay 中）
    */
    int readonly;  // 是否处于只读模式（replay / recovery）
};
// API 层用
#define RETURN_API_ERR(code) \
    do {                     \
        store->readonly = 1; \
        return (code);       \
    } while (0)

// 内部逻辑用
#define RETURN_ERR(code) \
    do {                 \
        return (code);   \
    } while (0)

static int kvstore_open_log(kvstore* store, const char* path);
static int kvstore_replay_log(kvstore* store);
static int kvstore_log_put(kvstore* store, int key, long value);
static int kvstore_log_del(kvstore* store, int key);
static int kvstore_load_snapshot(kvstore* store);
static int kvstore_create_snapshot(kvstore* store);
// static int kvstore_snapshot_exists(const char* path);
static int kvstore_log_header(const char* line);
static int kvstore_apply_put_internal(bptree* tree, int key, long value, kvstore_mode_t mode);  // mode 是为了识别模式，重放模式下不打印
static int kvstore_apply_del_internal(bptree* tree, int key);
const char* kvstore_strerror(int err);
static void kvstore_maybe_compact(kvstore* store);
static int kvstore_apply_put(kvstore* store, int key, long value);
static int kvstore_apply_del(kvstore* store, int key);

uint32_t crc32(const char* s);

// 创建 KVstore
kvstore* kvstore_create(const char* log_path) {
    kvstore* store = malloc(sizeof(*store));
    if (!store) return NULL;

    /* 1. 初始化为可 destroy 状态 */
    store->tree = NULL;
    store->log_fp = NULL;
    store->log_size = 0;
    store->ops_count = 0;
    store->mode = KVSTORE_MODE_NORMAL;
    store->readonly = 0;

    /* 2. 创建 B+ 树 */
    store->tree = bptree_create();
    if (!store->tree) goto fail;

    /* 3. 加载 snapshot（允许不存在） */
    kvstore_load_snapshot(store);

    /* 4. 打开 WAL（内部负责写 header） */
    if (kvstore_open_log(store, log_path) != KVSTORE_OK) {
        goto fail;
    }

    /* 5. replay WAL（现在 log_fp 一定合法） */
    if (kvstore_replay_log(store) != KVSTORE_OK) {
        fprintf(stderr, "[DEBUG] replay failed\n");
        goto fail;
    }

    return store;

fail:
    kvstore_destroy(store);
    return NULL;
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
    if (!store) {
        return;
    }

    /* 1. 关闭 WAL 日志 */
    if (store->log_fp) {
        fclose(store->log_fp);
        store->log_fp = NULL;
    }

    /* 2. 销毁 B+ 树 */
    if (store->tree) {
        bptree_destroy(store->tree);
        store->tree = NULL;
    }

    /* 3. 释放 store 本体 */
    free(store);
}

// 插入数据到 KVstore
int kvstore_insert(kvstore* store, int key, long value) {
    // 1. 基础检查
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 权限检查：只读模式下拒接插入操作
    if (store->readonly) return KVSTORE_ERR_READONLY;

    // 3. 先写日志（WAL）
    if (kvstore_log_put(store, key, value) != 0) {
        return KVSTORE_ERR_IO;
    }

    // 4. 再应用到内存（Apply）- 落实
    if (kvstore_apply_put_internal(store->tree, key, value, store->mode) != KVSTORE_OK) {
        return KVSTORE_ERR_INTERNAL;
    }

    // 5. 维护工作
    store->ops_count++;
    kvstore_maybe_compact(store);  // 检查是否需要压缩

    return KVSTORE_OK;
}

// 查找数据
int kvstore_search(kvstore* store, int key, long* value) {
    fprintf(stderr, "[DEBUG] before search, tree=%p\n", store->tree);
    return bptree_search(store->tree, key, value);
}

/**
 * API: 对外 PUT
 * 先 WAL, 再 apply
 *  - WAL 已落盘 -> replay 可恢复
 *  - apply 只在内存 -> 可重做
 */
int kvstore_put(kvstore* store, int key, long value) {
    if (!store)
        return KVSTORE_ERR_NULL;

    if (store->mode != KVSTORE_MODE_NORMAL)
        return KVSTORE_ERR_READONLY;

    if (kvstore_log_put(store, key, value) != KVSTORE_OK)
        return KVSTORE_ERR_IO;

    store->ops_count++;
    kvstore_maybe_compact(store);
    return kvstore_apply_put(store, key, value);
}

/**
 * API: del
 */
int kvstore_del(kvstore* store, int key) {
    // 1. 基础检查
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 权限检查：只读模式下拒接删除操作
    if (store->readonly) return KVSTORE_ERR_READONLY;

    // 3. 先写日志（WAL）
    if (kvstore_log_del(store, key) != 0) return KVSTORE_ERR_IO;

    store->ops_count++;
    kvstore_maybe_compact(store);

    return kvstore_apply_del(store, key);
}

// apply 层，只负责“把一次 PUT 应用到内存”
static int kvstore_apply_put(kvstore* store, int key, long value) {
    if (!store || !store->tree) return KVSTORE_ERR_NULL;
    return kvstore_apply_put_internal(store->tree, key, value, store->mode);
}

static int kvstore_apply_del(kvstore* store, int key) {
    if (!store || !store->tree) return KVSTORE_ERR_NULL;
    return kvstore_apply_del_internal(store->tree, key);
}

/**
 * 内部使用：只修改内存结构，不写 WAL
 *  - 无论是重放旧日志，还是正常写入，最终都要调用它
 *  - Replay 期间，数据库正是处于 readonly 模式，此处不能检查 readonly !!!
 *      否则，启动恢复时所有的旧数据都会被拦截，导致恢复失败
 */
static int kvstore_apply_put_internal(bptree* tree, int key, long value, kvstore_mode_t mode) {
    if (!tree) return KVSTORE_ERR_NULL;

    // fprintf(stderr, "[REPLAY] apply PUT %d = %ld\n", key, value);
    return bptree_insert(tree, key, value);
    int ret = bptree_insert(tree, key, value);
    if (ret == BPTREE_OK && mode == KVSTORE_MODE_NORMAL) {
        // 只有在非重放模式下才打印
        printf("键 %d 已存在，已更新值为 %ld\n", key, value);
    }
}

/**
 * 内部使用：del
 */
static int kvstore_apply_del_internal(bptree* tree, int key) {
    if (!tree) return KVSTORE_ERR_NULL;

    return bptree_delete(tree, key);
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
    if (!store || !path) return KVSTORE_ERR_NULL;

    FILE* fp = fopen(path, "a+");  // a+: 不存在则创建， 存在则读写
    if (!fp) {
        perror("fopen log failed");
        return KVSTORE_ERR_IO;
    }

    store->log_fp = fp;
    snprintf(store->log_path, sizeof(store->log_path), "%s", path);

    // 明确跳到文件末尾，判断大小
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);

    if (size == 0) {
        // 第一次创建日志，写 header
        fprintf(fp, "%s\n", KVSTORE_LOG_VERSION);
        fflush(fp);
    }

    // replay 前统一从头开始读
    fseek(fp, 0, SEEK_SET);

    return KVSTORE_OK;
}

/**
 * 写日志函数
 * PUT
 */
static int kvstore_log_put(kvstore* store, int key, long value) {
    if (!store->log_fp) return KVSTORE_ERR_NULL;

    char buf[128];
    snprintf(buf, sizeof(buf), "PUT %d %ld", key, value);
    uint32_t crc = crc32(buf);

    int bytes = fprintf(store->log_fp, "%s|%u\n", buf, crc);
    fflush(store->log_fp);

    store->ops_count += 1;
    store->log_size += bytes;

    return KVSTORE_OK;
}

/**
 * 删除日志函数
 * DEL
 */
static int kvstore_log_del(kvstore* store, int key) {
    if (!store->log_fp) return KVSTORE_ERR_NULL;

    char buf[128];
    snprintf(buf, sizeof(buf), "DEL %d", key);
    uint32_t crc = crc32(buf);

    int bytes = fprintf(store->log_fp, "%s|%u\n", buf, crc);
    fflush(store->log_fp);

    store->log_size += bytes;
    store->ops_count += 1;

    return KVSTORE_OK;
}

/**
 * 数据的“翻译官” - dump :转储
 * 拿到一条数据，按格式写进文件
 */
static int compact_write_cb(int key, long value, void* arg) {
    FILE* fp = (FILE*)arg;  // 强转，把万能指针还原为文件指针

    // 1. 准备缓冲区
    char payload[128];
    snprintf(payload, sizeof(payload), "PUT %d %ld", key, value);

    // 2. 调用 crc32()
    uint32_t crc_val = crc32(payload);

    // 3. 写入完整格式: Payload | CRC\n
    fprintf(fp, "%s|%u\n", payload, crc_val);

    return KVSTORE_OK;
}

// kvstore 完全不知道 B+ 树内部结构
/**
 * 日志压缩 - 数据库的“返老还童术”
 */
int kvstore_compact(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    const char* tmp_path = "data.compact";
    const char* data_path = store->log_path;

    /* 1. 打开临时文件 */
    FILE* fp = fopen("data.compact", "w");
    if (!fp) {
        perror("fopen compact");
        return KVSTORE_ERR_IO;
    }

    /* 必须先写 Header！ */
    fprintf(fp, "%s\n", KVSTORE_LOG_VERSION);

    /* 2. 顺序扫描 B+ 树 */
    if (bptree_scan(store->tree, compact_write_cb, fp) != 0) {
        fclose(fp);
        return KVSTORE_ERR_INTERNAL;
    }

    /* 3. 刷盘，保证落盘 */
    fflush(fp);         // 把 C 语言层面的缓冲区数据堆推给操作系统内核
    fsync(fileno(fp));  // 让磁盘磁头动起来，把物理电信号写进硬盘扇区
    fclose(fp);

    /* 4. 原子替换旧数据文件 */
    FILE* old_fp = store->log_fp;
    store->log_fp = NULL;

    if (rename(tmp_path, data_path) != 0) {  // 在 Linux / Unix 上：rename 是原子操作
        perror("rename");
        store->log_fp = old_fp;  // 回滚
        return KVSTORE_ERR_INTERNAL;
    }
    fclose(old_fp);

    /* 5. 重新打开 log 文件 */
    // 就的连接断开了，重新打开已经“瘦身”的新文件
    store->log_fp = fopen(data_path, "a+");
    if (!store->log_fp) {
        perror("reopen data.log");
        return KVSTORE_ERR_INTERNAL;
    }

    kvstore_create_snapshot(store);
    return KVSTORE_OK;
}

/**
 * 自动触发器
 */
static void kvstore_maybe_compact(kvstore* store) {
    if (!store) return;

    // store->log_size - 空间维度，日志文件太大，需要压缩
    // store->ops_count - 时间/效率维度，防止“重放”太慢
    if (store->log_size >= KVSTORE_MAX_LOG_SIZE ||
        store->ops_count >= KVSTORE_MAX_OPS) {
        kvstore_compact(store);

        // 重置计数器
        store->log_size = 0;
        store->ops_count = 0;
    }
}

/**
 * 写回调（复用compaction 思路）
 */
static int snapshot_write_cb(int key, long value, void* arg) {
    FILE* fp = (FILE*)arg;

    // 1. 构造 Payload
    char payload[128];
    snprintf(payload, sizeof(payload), "PUT %d %ld", key, value);

    // 2. 计算 CRC (必须加，否则 replay 不认识)
    uint32_t crc_val = crc32(payload);

    // 3. 按照 V3 标准格式写入：数据 | 校验码\n
    fprintf(fp, "%s|%u\n", payload, crc_val);

    return KVSTORE_OK;
}

/**
 * 创建 snapshot(核心)
 *  - 快照本质上就是内存数据的“全量备份”
 *  - 在日志压缩后创建！
 */
static int kvstore_create_snapshot(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    const char* tmp = "data.snapshot.tmp";
    const char* snap = "data.snapshot";  // 冷启动快照

    FILE* fp = fopen(tmp, "w");
    if (!fp) {
        perror("fopen snapshot");
        return KVSTORE_ERR_INTERNAL;
    }

    if (bptree_scan(store->tree, snapshot_write_cb, fp) != 0) {
        fclose(fp);
        return KVSTORE_ERR_INTERNAL;
    }

    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    if (rename(tmp, snap) != 0) {
        perror("rename snapshot");
        return KVSTORE_ERR_NULL;
    }
    return KVSTORE_OK;
}

/**
 * 加载（读） -- 冷启动加速的关键
 */
static int kvstore_load_snapshot(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    FILE* fp = fopen("data.snapshot", "r");
    if (!fp) return KVSTORE_OK;  // snapshot 不存在，正常情况

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        int key;
        long value;

        if (sscanf(line, "PUT %d %ld", &key, &value) == 2) {
            kvstore_apply_put_internal(store->tree, key, value, store->mode);
        }
    }

    fclose(fp);
    return KVSTORE_OK;
}

// CRC 函数（简单版）- 循环冗余校验
uint32_t crc32(const char* s) {
    uint32_t crc = 0xFFFFFFFF;
    while (*s) {
        crc ^= (unsigned char)*s++;
        for (int i = 0; i < 8; i++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }

    return ~crc;
}

// 先校验 Header + Version
static int kvstore_log_header(const char* line) {
    if (strncmp(line, KVSTORE_LOG_VERSION, strlen(KVSTORE_LOG_VERSION)) != 0) {
        fprintf(stderr, "Invalid log version. Expected: %s\n", KVSTORE_LOG_VERSION);
        return KVSTORE_ERR_CORRUPTED;
    }

    return KVSTORE_OK;
}

/**
 * 校验单行日志的完整性
 *  - 成功返回 1， 失败返回 0
 * 注意：此函数会修改 line 字符串（就地切割）
 */
static int kvstore_crc_check(const char* payload, const char* crc_str) {
    if (!payload || !crc_str) return 0;

    char* end = NULL;
    uint32_t crc_stored = (uint32_t)strtoul(crc_str, &end, 10);

    /* crc 字段非法 */
    if (end == crc_str || *end != '\0')
        return 0;

    uint32_t crc_calc = crc32(payload);
    return crc_calc == crc_stored;
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
 * 校验每一条日志
 *  - replay 不调用 public API, 所以不会触发 readonly 检查
 *  - apply 是 “特权通道”
 */
static int kvstore_replay_log(kvstore* store) {
    if (!store || !store->tree || !store->log_fp) RETURN_ERR(KVSTORE_ERR_NULL);

    store->mode = KVSTORE_MODE_REPLAY;

    char line[256];
    int rc = KVSTORE_OK;

    rewind(store->log_fp);

    // 读取并校验 header
    if (!fgets(line, sizeof(line), store->log_fp)) {
        // 空日志，允许
        goto out;
    }

    if (kvstore_log_header(line) != KVSTORE_OK) {
        rc = KVSTORE_ERR_CORRUPTED;
        goto out;
    }

    // replay body
    while (fgets(line, sizeof(line), store->log_fp)) {
        /* 1. 跳过空行 */
        if (line[0] == '\n' || line[0] == '\r' || line[0] == ' ')
            continue;

        /* 2. 去掉末尾换行 */
        line[strcspn(line, "\r\n")] = '\0';

        /* 3. split payload | crc */
        char* sep = strchr(line, '|');
        if (!sep) {
            // 容忍尾行被破坏
            // rc = KVSTORE_ERR_CORRUPTED;

            // 不报错，直接当作读完
            rc = KVSTORE_OK;
            break;
        }

        *sep = '\0';
        char* crc_str = sep + 1;

        /* 4. CRC 校验（只校验 payload） */
        if (!kvstore_crc_check(line, crc_str)) {
            if (feof(store->log_fp)) {
                printf("[DEBUG] 容忍末尾不完整的行：%s\n", line);
                rc = KVSTORE_OK;
                break;
            }

            rc = KVSTORE_ERR_CORRUPTED;
            fprintf(stderr, "[DEBUG] CRC failed! Payload: [%s], Stored CRC: [%s]\n", line, crc_str);
            break;
        }

        /* 5. apply */
        int key;
        long val;

        if (sscanf(line, "PUT %d %ld", &key, &val) == 2) {
            kvstore_apply_put_internal(store->tree, key, val, store->mode);
        } else if (sscanf(line, "DEL %d", &key) == 1) {
            kvstore_apply_del_internal(store->tree, key);
        } else {
            rc = KVSTORE_ERR_CORRUPTED;
            break;
        }
    }

out:
    store->mode = KVSTORE_MODE_NORMAL;
    return rc;
}

/**
 * 错误码 -> 人类可读字符串
 *
 * 外部调用者使用     if rc != KVSTORE_OK ...
 */
const char* kvstore_strerror(int err) {
    switch (err) {
        case KVSTORE_OK:
            return "OK";
        case KVSTORE_ERR_NULL:
            return "null pointer";
        case KVSTORE_ERR_INTERNAL:
            return "internal error";
        case KVSTORE_ERR_IO:
            return "io error";
        case KVSTORE_ERR_READONLY:
            return "read-only mode";
        case KVSTORE_ERR_NOT_FOUND:
            return "key not found";
        case KVSTORE_ERR_CORRUPTED:
            return "data corrupted";
        default:
            return "unknown error";
    }
}

// int kvstore_snapshot_exists(const char* path) {
//     return access(path, F_OK);
// }

// 调试专用：手动切换数据库模式
void kvstore_debug_set_mode(kvstore* store, kvstore_mode_t mode) {
    if (store) {
        store->mode = mode;
        fprintf(stderr, "[debug] 手动切换至 %d模式\n", mode);
    }
}
