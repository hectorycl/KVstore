#include "kvstore.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// ======== 内部结构体的定义 =======
/**
 * “系统状态聚合体”
 * kvstore 只是“持有”一个存储引擎的实例，kvstore ≠ 存储引擎！
 *  - 内存状态：tree
 *  - 持久化状态：log
 *  - 运行时状态：mode / readonly / 统计
 *
 *  - tree: B+ 树指针
 *  - log_fp: 日志文件指针
 *  - log_size: 当前日志大小（字节）
 *  - ops_count: 自上次 compaction 以来的操作次数
 *  - readonly: 正常：0  只读：1
 *
 *  - size_t: 为表示大小或计数而设计的无符号整数
 */
struct _kvstore {
    bptree* tree;

    FILE* log_fp;
    char log_path[256];

    kvstore_mode_t mode;

    size_t log_size;
    size_t ops_count;

    int readonly;  // replay / recovery 期间只读状态，禁止写
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

    /* 1. 初始化为可 destroy 状态  -> 让 kvstore->destroy 可以被无条件安全调用 */
    store->tree = NULL;
    store->log_fp = NULL;
    store->log_size = 0;
    store->ops_count = 0;
    store->mode = KVSTORE_MODE_NORMAL;
    store->readonly = 0;

    /* 2. 创建 B+ 树 */  // ，C 里处理多资源释放 -> goto fail
    store->tree = bptree_create();
    if (!store->tree) goto fail;

    /* 3. 加载 snapshot（允许不存在） */
    kvstore_load_snapshot(store);

    /* 4. 打开 WAL */
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
 * 释放 kvstore 持有的所有资源，并保证无泄漏
 *
 * 功能：
 *  - close WAL log file if opened
 *  - destroy 潜在的 B+ 树
 *  - free the kvstore structure
 *
 * it is safe to call this function with a NULL pointer
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

/**
 * kvstore_search - 对外查询接口（查找 key 对应的 value）
 *
 * 设计原则：
 *  1. 这是 kvstore 的“读路径（read path）”
 *  2. 读操作允许在只读模式（readonly / replay 模式）下执行
 *  3. 实际的数据查找完全交给底层索引结构（B+ 树）
 *
 * 注意：
 *  - search 本身不关心系统模式（NORMAL / READONLY / REPLAY）
 *  - 只要系统中存在索引结构，就可以安全查询
 */
int kvstore_search(kvstore* store, int key, long* value) {
    if (!store || !store->tree) return KVSTORE_ERR_NULL;

    return bptree_search(store->tree, key, value);
}

/**
 * kvstore_put 对外写接口 （PUT）
 *
 * 写入一条 key-value 记录到 kvstore
 *
 * [架构语义]
 * 用户唯一合法的写入口，负责协调一次“完整写操作”
 *
 *  1. 参数状态校验（API 层职责）
 *  2. 写 WAL (Write-Ahead Log, 预写日志)
 *  3. 维护系统运行状态（操作技术 / 压缩判断）
 *  4. 调用 Apply Layer 修改内存结构
 *
 * [关键不变量]
 *  - 所有用户写操作，必须先成功写 WAL，才能修改内存
 *  - Apply Layer（内部 put_internal） 只负责“状态变更”，不做权限和持久化判断
 *
 */
int kvstore_put(kvstore* store, int key, long value) {
    // API 层：基本合法性校验
    if (!store)
        return KVSTORE_ERR_NULL;

    // API 层：写权限检查（replay / recovery 期间禁止写入）
    if (store->mode != KVSTORE_MODE_NORMAL)
        return KVSTORE_ERR_READONLY;

    // 持久化层：先写 WAL, 保证崩溃后可 replay
    if (kvstore_log_put(store, key, value) != KVSTORE_OK)
        return KVSTORE_ERR_IO;

    // 运行态维护：记录操作次数，用于触发 compaction
    store->ops_count++;
    kvstore_maybe_compact(store);

    // Apply Layer: 真正修改内存中的 B+ Tree
    return kvstore_apply_put(store, key, value);
}

/**
 * kvstore_del - 对外 DEL 接口（删除一个 key）
 *
 * 设计原则：
 *  1. 对外 API 永不直接修改内存数据结构
 *  2. 所有写操作必须遵循：WAL → apply 的顺序
 *
 * 注意：
 *  - 若 WAL 写入成功但是 apply 尚未执行时系统崩溃，
 *    启动时可通过 replay WAL 恢复该操作
 *  - apply 层不做权限与模式判断，只负责“该内存”
 *
 * 设计理解：
 *  - kvstore_del 本身并不删除数据，真正删除发生在 apply 层。
 */
int kvstore_del(kvstore* store, int key) {
    // 1. 基础检查
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 权限 / 模式检查：只读模式禁止删除
    if (store->readonly) return KVSTORE_ERR_READONLY;

    // 3. 先写日志（WAL）
    if (kvstore_log_del(store, key) != 0) return KVSTORE_ERR_IO;

    // 4. 运行态维护
    store->ops_count++;
    kvstore_maybe_compact(store);

    // 5. 应用到内存结构
    return kvstore_apply_del(store, key);
}

// apply 层，只负责“把一次 PUT 应用到内存”
/**
 * apply(执行) a PUT operation to the in-memory state（内存状态）
 *
 * 功能：
 *  - 直接修改 B+ 树，不执行人格日志记录或持久化操作。
 *  - 它同时用于普通写路径和 WAL 重放
 */
static int kvstore_apply_put(kvstore* store, int key, long value) {
    if (!store || !store->tree) return KVSTORE_ERR_NULL;
    return kvstore_apply_put_internal(store->tree, key, value, store->mode);
}

/**
 * Apply a DELETE operation to the in-memory state.
 */
static int kvstore_apply_del(kvstore* store, int key) {
    if (!store || !store->tree) return KVSTORE_ERR_NULL;
    return kvstore_apply_del_internal(store->tree, key);
}

/**
 * Apply a PUT operation directly to the B+ tree
 *
 * 仅修改内存中的状态，
 * it must NOT:
 *  - write WAL
 *  - check readonly
 *
 * @param mode current kvstore mode (normal / repaly)
 */
static int kvstore_apply_put_internal(bptree* tree, int key, long value, kvstore_mode_t mode) {
    if (!tree) return KVSTORE_ERR_NULL;

    int ret = bptree_insert(tree, key, value);
    if (ret == BPTREE_OK && mode == KVSTORE_MODE_NORMAL) {
        // 只有在非重放模式下才打印
        printf("键 %d 已存在，已更新值为 %ld\n", key, value);
    }

    return ret;
}

/**
 * Apply a DEL operation directly to the B+ tree
 */
static int kvstore_apply_del_internal(bptree* tree, int key) {
    if (!tree) return KVSTORE_ERR_NULL;

    return bptree_delete(tree, key);
}

/**
 * 打开或创建 WAL file for kvstore
 *
 * 功能：
 *  - open(打开) the log file in append/update mode ("a+")
 *  - record(记录) the log file path in kvstore
 *  - initilaize(初始化) the log header if the file is newly creaeted
 *  - perpare(准备) the file pointer(文件指针) for subsequent log replay
 *
 * @param
 *  - store pointer to kvstore instance
 *  - path  path to the WAL log file
 *
 * @return KVSTORE_OK on success, or a negative error code on failure
 */
static int kvstore_open_log(kvstore* store, const char* path) {
    if (!store) return KVSTORE_ERR_NULL;
    if (!path) return KVSTORE_ERR_INTERNAL;

    // open
    FILE* fp = fopen(path, "a+");
    if (!fp) {
        perror("fopen log failed");
        return KVSTORE_ERR_IO;
    }

    // record
    store->log_fp = fp;
    snprintf(store->log_path, sizeof(store->log_path), "%s", path);

    // initialize
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    if (size == 0) {
        fprintf(fp, "%s\n", KVSTORE_LOG_VERSION);
        fflush(fp);
    }

    // reset file position for log replay
    fseek(fp, 0, SEEK_SET);

    return KVSTORE_OK;
}

/**
 * kvstore_log_put - 记录一次 PUT 操作到 WAL 日志
 *
 * 设计定位：
 *  - WAL 层函数
 *  - 只负责“把一次 PUT 操作顺序写入磁盘日志”
 *  - 不修改内存结构，不调用 B+ 树
 *
 * 日志格式
 *      PUT <key>|<crc32>\n
 *
 * 执行流程：
 *  1. 校验日志文件是否已打开
 *  2. 构造日志内容字符串
 *  3. 计算 CRC32 校验值
 *  4. 写入日志文件并强制刷盘
 *  5. 更新日志统计信息（大小 / 操作次数）
 *
 * snprintf(): 将变量格式化为字符串，并安全写入缓冲区
 *
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
 * kvstore_log_del - 记录一次 DEL 操作到 WAL 日志
 *
 * 设计定位：
 *  - WAL 层删除日志记录函数
 *
 * 日志格式：
 *      DEL <key>|<crc32>\n
 *
 */
static int kvstore_log_del(kvstore* store, int key) {
    if (!store->log_fp) return KVSTORE_ERR_NULL;

    char buf[128];
    snprintf(buf, sizeof(buf), "DEL %d", key);
    uint32_t crc = crc32(buf);

    int bytes = fprintf(store->log_fp, "%s|%u\n", buf, crc);
    fflush(store->log_fp);  // fsync(fileno(store->log_fp)); // 强制操作系统把数据写进物理磁盘

    store->log_size += bytes;
    store->ops_count += 1;

    return KVSTORE_OK;
}

/**
 * Compaction 写入回调
 * 将当前 B+ Tree 中的 key-value
 * 以 WAL 格式写入新的 compacted log
 */
static int compact_write_cb(int key, long value, void* arg) {
    FILE* fp = (FILE*)arg;  // 强转，把万能指针还原为文件指针

    // 1. 准备缓冲区
    char payload[128];
    snprintf(payload, sizeof(payload), "PUT %d %ld", key, value);

    // 2. 调用 crc32()
    uint32_t crc_val = crc32(payload);

    // 3. 写入完整格式: Payload | CRC\n
    if (fprintf(fp, "%s|%u\n", payload, crc_val) < 0) {
        return KVSTORE_ERR_INTERNAL;
    }

    return KVSTORE_OK;
}

// kvstore 完全不知道 B+ 树内部结构
/**
 * kvstore_cmopact - 日志压缩(Compaction)
 *
 * 核心思想：
 *  - 将当前内存中的完整状态（B+ 树） 重写为一个新的 WAL
 *  - 丢弃历史冗余日志，降低 replay 成本
 *
 * 崩溃安全性保证（crash-safe）:   → crash consistency（崩溃一致性）
 *  - 使用临时文件 + rename 的原子替换语义
 *  - 任意时刻宕机，磁盘上要么是旧 WAL,要么是新的 WAL
 */
int kvstore_compact(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    const char* tmp_path = "data.compact";
    const char* data_path = store->log_path;

    /* 1. 创建临时 WAL 文件 */
    FILE* fp = fopen("data.compact", "w");
    if (!fp) {
        perror("fopen compact");
        return KVSTORE_ERR_IO;
    }

    /* 2. 写 WAL Header(必须) */
    fprintf(fp, "%s\n", KVSTORE_LOG_VERSION);

    /* 3.全量扫描内存状态，生成新的 WAL */
    if (bptree_scan(store->tree, compact_write_cb, fp) != 0) {
        fclose(fp);
        unlink(tmp_path);
        return KVSTORE_ERR_INTERNAL;
    }

    /* 4. 刷盘，保证物理落盘 */
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    /* 5. 原子替换旧 WAL */
    FILE* old_fp = store->log_fp;
    store->log_fp = NULL;

    if (rename(tmp_path, data_path) != 0) {
        perror("rename");
        store->log_fp = old_fp;  // 回滚
        return KVSTORE_ERR_INTERNAL;
    }
    fclose(old_fp);

    /* 6. 重新打开 WAL 文件 */
    store->log_fp = fopen(data_path, "a+");
    if (!store->log_fp) {
        perror("reopen data.log");
        return KVSTORE_ERR_INTERNAL;
    }

    /* 7. 创建 snapshot (性能优化，不影响正确性) */
    kvstore_create_snapshot(store);

    return KVSTORE_OK;
}

/**
 * kvstore_maybe_compact - 判断是否需要进行 compaction(日志压缩)
 *
 * 触发条件（满足任一即可）：
 *  - WAL 文件过大（空间维度）
 *  - 操作次数过多（replay 成本过高）
 *
 * 设计说明：
 *  - compaction 属于维护行为，不影响 PUT / DEL 正确性
 *  - 即使 compaction 失败，WAL 仍然可保证数据安全
 */
static void kvstore_maybe_compact(kvstore* store) {
    if (!store) return;

    if (store->log_size >= KVSTORE_MAX_LOG_SIZE ||
        store->ops_count >= KVSTORE_MAX_OPS) {
        int rc = kvstore_compact(store);
        if (rc == KVSTORE_OK) {
            // compaction 成功，进入新的 epoch
            // 重置计数器
            store->log_size = 0;
            store->ops_count = 0;
        } else {
            // compaction 失败
            // 不重置计数器，后续操作仍可再次尝试
            fprintf(stderr, "[WARN] kvstore compaction failed, will retry later\n");
        }
    }
}

/**
 * snapshot 写回调函数
 * 
 * 用于将 B+ 树重点最终状态序列化为 snapshot 文件
 * 
 * 设计要点：
 *  - snapshot 使用与 WAL 相同的记录格式 （PUT key value|CRC）
 *  - 保持磁盘格式统一，便于复用 replay / compact 逻辑
 *  - snapshot 仅用于冷启动加速，不参与数据正确性保证
 */
static int snapshot_write_cb(int key, long value, void* arg) {
    FILE* fp = (FILE*)arg;

    // 1. 构造操作语义（与 WAL 保持一致）
    char payload[128];
    snprintf(payload, sizeof(payload), "PUT %d %ld", key, value);

    // 2. 计算 CRC 校验码（用于格式统一与潜在校验）
    uint32_t crc_val = crc32(payload);

    // 3. 写入 snapshot 记录（一行一条完整记录）
    fprintf(fp, "%s|%u\n", payload, crc_val);

    return KVSTORE_OK;
}

/**
 * kvstore_create_snapshot - 创建内存状态的快照（snapshot）
 *
 * 功能：
 *  - 将当前 B+ Tree 中的全部 key-value
 *    以确定性顺序写入快照文件
 *  - 用于 WAL 压缩后的状态固化
 *
 * 架构定位：
 *  - 系统维护函数（非对外 API）
 *  - 通常在 compaction 后调用
 *  - 提供 crash-safe 的全量状态持久化
 *
 * 设计要点：
 *  - 采用 tmp 文件 + rename 的原子替换策略
 *  - 使用 B+ Tree scan + callback 解耦策略 （还要重点学习）
 *  - 显示 flush + fsync 保证落盘
 */
static int kvstore_create_snapshot(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    const char* tmp = "data.snapshot.tmp";
    const char* snap = "data.snapshot";  // 冷启动快照

    // 1. 打开临时快照文件
    FILE* fp = fopen(tmp, "w");
    if (!fp) {
        perror("fopen snapshot");
        return KVSTORE_ERR_INTERNAL;
    }

    // 2. 遍历 B+ Tree, 写出所有 key-value
    if (bptree_scan(store->tree, snapshot_write_cb, fp) != 0) {
        fclose(fp);
        return KVSTORE_ERR_INTERNAL;
    }

    // 强制刷新到磁盘，保证 crash-safe
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    // 4. 原子替换正式快照文件
    if (rename(tmp, snap) != 0) {
        perror("rename snapshot");
        return KVSTORE_ERR_NULL;
    }
    return KVSTORE_OK;
}

/**
 * kvstore_load_snap - 加载 snapshot（冷启动加速）
 *
 * 设计说明：
 *  - snapshot 是内存 KV 状态的“全量物化”
 *  - 只包含 PUT, 不包含 DEL
 *  - snapshot 不存在是合法情况（首次启动）
 *  - 加载过程不写 WAL, 不做权限检查
 *
 *
 * 启动恢复流程：
 *  1. load_snapshot()  -> 快速恢复到最近状态
 *  2. replay_log()     -> 重放 snapshot 之后的 WAL
 */
static int kvstore_load_snapshot(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    FILE* fp = fopen("data.snapshot", "r");
    if (!fp) {
        // snapshot 不存在，允许慢启动
        return KVSTORE_OK;
    }

    char line[256];
    int key;
    long value;

    // snapshot 加载属于恢复路径，进入 REPLAY 模式
    kvstore_mode_t old_mode = store->mode;
    store->mode = KVSTORE_MODE_REPLAY;

    while (fgets(line, sizeof(line), fp)) {
        // 跳过空行
        if (line[0] == '\n' || line[0] == '\r')
            continue;

        // snapshot 行格式固定为：PUT key value
        if (sscanf(line, "PUT %d %ld", &key, &value) == 2) {
            kvstore_apply_put_internal(store->tree, key, value, store->mode);
        }

        // 非法行： 忽略，snapshot 是可信文件
    }

    store->mode = old_mode;
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
 * kvstore_replay_log - 日志重放（WAL Replay / Recovery）
 *
 * 功能：
 *  - 从 WAL 文件中顺序读取历史操作（PUT / DEL）
 *  - 校验每一条日志记录完整性（CRC）
 *  - 将合法操作重新 apply 到内存中的 B+ Tree
 *
 * 设计定位：
 *  - 属于“恢复阶段的特权通道”
 *  - 只在 kvstore_create() 中被调用
 *  - 不允许被用户或 Public API 直接调用
 *
 * 设计约束：
 *  - 不写 WAL (否则会造成日志膨胀)
 *  - 不走 Public API (绕过 readonly / 权限检查)
 *  - 只调用 apply_internal 系列函数
 *
 */
static int kvstore_replay_log(kvstore* store) {
    // 0. 基础合法性检查
    if (!store || !store->tree || !store->log_fp) RETURN_ERR(KVSTORE_ERR_NULL);

    // 1. 切换到 REPLAY 模式
    store->mode = KVSTORE_MODE_REPLAY;

    char line[256];
    int rc = KVSTORE_OK;

    rewind(store->log_fp);

    // 2. 读取并校验 日志头
    if (!fgets(line, sizeof(line), store->log_fp)) {
        // 空日志是合法（新建数据库）
        goto out;
    }

    if (kvstore_log_header(line) != KVSTORE_OK) {
        rc = KVSTORE_ERR_CORRUPTED;
        goto out;
    }

    // 3. 逐行重放日志体
    while (fgets(line, sizeof(line), store->log_fp)) {
        /* 3.1 跳过空行 */
        if (line[0] == '\n' || line[0] == '\r' || line[0] == ' ')
            continue;

        /* 3.2 去掉末尾换行（\n 或 \r\n） */
        line[strcspn(line, "\r\n")] = '\0';

        /* 3.3 拆分 payload | crc */
        char* sep = strchr(line, '|');
        if (!sep) {
            // 容忍尾行被破坏，不报错，直接当作读完
            rc = KVSTORE_OK;
            break;
        }

        *sep = '\0';
        char* crc_str = sep + 1;

        /* 3.4 CRC 校验（只校验 payload） */
        if (!kvstore_crc_check(line, crc_str)) {
            if (feof(store->log_fp)) {
                rc = KVSTORE_OK;
                break;
            }

            // 中间损坏，无法继续恢复
            rc = KVSTORE_ERR_CORRUPTED;
            fprintf(stderr, "[DEBUG] CRC failed! Payload: [%s], Stored CRC: [%s]\n", line, crc_str);
            break;
        }

        /* 3.5 根据日志内容 apply 到内存 */
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
