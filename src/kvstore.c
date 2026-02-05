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

    /* v4 新增 */
    kvstore_state_t state;

    /* v3, 降级为实现细节 */
    kvstore_mode_t mode;
    int readonly;  // replay / recovery 期间只读状态，禁止写

    size_t log_size;
    size_t ops_count;
};

typedef struct {
    int applied;    // 成功应用
    int skipped;    // 跳过 / hulue
    int corrupted;  // 损坏 / 错误
} replay_stats;

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

uint32_t crc32(const char* s);

static int kvstore_replay_log(kvstore* store);
static int kvstore_log_put(kvstore* store, int key, long value);
static int kvstore_log_del(kvstore* store, int key);
static int kvstore_load_snapshot(kvstore* store);
// static int kvstore_create_snapshot(kvstore* store);
static int kvstore_log_header(const char* line);
static int kvstore_apply_put_internal(bptree* tree, int key, long value);
static int kvstore_apply_del_internal(bptree* tree, int key);
const char* kvstore_strerror(int err);
static void kvstore_maybe_compact(kvstore* store);
static int kvstore_apply_put(kvstore* store, int key, long value);
static int kvstore_apply_del(kvstore* store, int key);
static int compact_write_cb(int key, long value, void* arg);
static int snapshot_write_cb(int key, long value, void* arg);
int kvstore_compact(kvstore* store);
static int kvstore_compact_internal(kvstore* store);

// 语义统一
static int kvstore_enter_recovering(kvstore* store);
static int kvstore_enter_ready(kvstore* store);
static int kvstore_enter_failed(kvstore* store);  // == corrupted
static int kvstore_enter_compaction(kvstore* store);
static void kvstore_exit_compaction(kvstore* store, kvstore_state_t prev);

// 原子一致性
static int kvstore_replay_put(kvstore* store, int key, long value);
static int kvstore_replay_del(kvstore* store, int key);
static int kvstore_exec_write(kvstore* store, kvstore_op_t op, int key, int value);
static int kvstore_state_allow(kvstore_state_t state, kvstore_op_t op);
static int kvstore_fatal(kvstore* store, int err);
static int kvstore_transit_state(kvstore* store, kvstore_state_t next);

static void kvstore_apply_state(kvstore* store);  // 根据 state, 统一设置 mode 和 readonly

/**
 * 系统进入不可恢复错误态，只允许 destroy
 */
static int kvstore_enter_failed(kvstore* store) {
    return kvstore_transit_state(store, KVSTORE_STATE_CORRUPTED);
}

/**
 * 系统完成恢复，可以对外提供完整服务
 */
static int kvstore_enter_ready(kvstore* store) {
    return kvstore_transit_state(store, KVSTORE_STATE_READY);
}

/**
 * 状态切换函数
 *  - 进入恢复期
 */
static int kvstore_enter_recovering(kvstore* store) {
    return kvstore_transit_state(store, KVSTORE_STATE_RECOVERING);
}

/**
 * kvstore_apply_state - 系统权限总闸：根据当前系统状态（State）刷新底层的运行模式（mode）与读写权
 *
 * 设计说明：
 *  - 外部逻辑只需变更 store->state
 *  - 调用本函数，将抽象的状态统一解析为具体的控制开关
 *
 * state
 *  - 系统级控制变量
 *  - 只在系统级流程节点发生变化，不能在普通业务里随便改
 *
 */
static void kvstore_apply_state(kvstore* store) {
    switch (store->state) {
        case KVSTORE_STATE_INIT:
            store->mode = KVSTORE_MODE_NORMAL;
            store->readonly = 1;
            break;

        case KVSTORE_STATE_RECOVERING:
            // recovering 期间，强制 mode = replay !
            store->mode = KVSTORE_MODE_REPLAY;
            store->readonly = 1;
            break;

        case KVSTORE_STATE_READY:
            store->mode = KVSTORE_MODE_NORMAL;
            store->readonly = 0;
            break;

        case KVSTORE_STATE_READONLY:
            store->mode = KVSTORE_MODE_NORMAL;
            store->readonly = 1;
            break;

        case KVSTORE_STATE_CLOSING:
        case KVSTORE_STATE_CORRUPTED:
            store->readonly = 1;
            break;

        default:
            // 其他情况，进入只读模式，保护数据。
            store->readonly = 1;
            break;
    }
}

// 创建 KVstore
kvstore* kvstore_create() {
    kvstore* store = malloc(sizeof(*store));
    if (!store) return NULL;

    store->tree = bptree_create();
    if (!store->tree) {
        free(store);
        return NULL;
    }

    store->state = KVSTORE_STATE_INIT;
    store->log_fp = NULL;
    store->log_size = 0;
    store->ops_count = 0;
    store->mode = KVSTORE_MODE_NORMAL;
    store->readonly = 0;

    return store;
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
    if (!store)
        return;

    /* 防御性：如果没 close，先 close */
    if (store->state != KVSTORE_STATE_CLOSED) {
        kvstore_close(store);
    }

    /* 1. 销毁 B+ 树 */
    if (store->tree) {
        bptree_destroy(store->tree);
        store->tree = NULL;
    }

    /* 2. 释放 store 本体 */
    free(store);
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
int kvstore_search(kvstore* store, int key, long* value) {  // kvstore_get
    if (!store || !store->tree) return KVSTORE_ERR_NULL;

    if (!kvstore_state_allow(store->state, KVSTORE_OP_GET)) {
        return KVSTORE_ERR_INTERNAL_STATE;
    }
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
 * exec 只给在线写用，replay 只做 apply
 */
int kvstore_put(kvstore* store, int key, long value) {
    return kvstore_exec_write(store, KVSTORE_OP_PUT, key, value);
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
    return kvstore_exec_write(store, KVSTORE_OP_DEL, key, 0);
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
    if (!store) return KVSTORE_ERR_NULL;

    if (!store->tree) return KVSTORE_ERR_INTERNAL_STATE;

    return kvstore_apply_put_internal(store->tree, key, value);
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
 */
static int kvstore_apply_put_internal(bptree* tree, int key, long value) {
    if (!tree) return KVSTORE_ERR_NULL;

    int ret = bptree_insert(tree, key, value);

    switch (ret) {
        case BPTREE_OK:
        case BPTREE_UPDATED:
            return KVSTORE_OK;

        case BPTREE_ERR:
            return KVSTORE_ERR_INTERNAL;

        default:
            return KVSTORE_ERR_INTERNAL;
    }
}

/**
 * Apply a DEL operation directly to the B+ tree
 */
static int kvstore_apply_del_internal(bptree* tree, int key) {
    if (!tree) return KVSTORE_ERR_NULL;

    return bptree_delete(tree, key);
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

    int fd = fileno(store->log_fp);
    if (fd >= 0)
        fsync(fd);

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

    int fd = fileno(store->log_fp);
    if (fd >= 0)
        fsync(fd);

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

/**
 * kvstore_compact - 日志压缩对外的公共入口（公有 API）
 *  - 给调用者语义级操作（这是数据库行为，不是文件操作！）
 *
 * 设计模式：包裹模式
 *  1. 现场保护：在执行前通过 prev 变量备份当前的系统状态
 *  2. 状态锁定：调用 enter_compaction 切换为只读，防止压缩期间数据被篡改
 *  3. 核心执行：将压缩工作委派给内部函数 compact_internal 处理
 *  4. 现场恢复：无论压缩成功与否，最终都通过 exit_compaction 恢复之前的系统状态
 *
 *  不会导致数据库永久锁死在只读模式
 */
int kvstore_compact(kvstore* store) {
    int ret;
    kvstore_state_t prev;

    if (!store) return KVSTORE_ERR_NULL;

    prev = store->state;

    ret = kvstore_enter_compaction(store);
    if (ret != KVSTORE_OK)
        return ret;

    ret = kvstore_compact_internal(store);

    kvstore_exit_compaction(store, prev);
    return ret;
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
static int kvstore_compact_internal(kvstore* store) {
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
int kvstore_create_snapshot(kvstore* store) {
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

    while (fgets(line, sizeof(line), fp)) {
        // 跳过空行
        if (line[0] == '\n' || line[0] == '\r')
            continue;

        // snapshot 行格式固定为：PUT key value
        if (sscanf(line, "PUT %d %ld", &key, &value) == 2) {
            kvstore_apply_put_internal(store->tree, key, value);
        }

        // 非法行： 忽略，snapshot 是可信文件
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
        return KVSTORE_ERR_WAL_CORRUPTED;
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
 *
 */
int kvstore_replay_put(kvstore* store, int key, long value) {
    if (!store) return KVSTORE_ERR_NULL;

    if (!kvstore_state_allow(store->state, KVSTORE_OP_REPLAY))
        return KVSTORE_ERR_INTERNAL_STATE;

    // replay 阶段， 只负责把 WAL 的事实重放到内存
    int ret = kvstore_apply_put(store, key, value);
    if (ret != KVSTORE_OK) {
        printf("Replay PUT failed: key=%d, err=%d", key, ret);
        return ret;
    }

    return KVSTORE_OK;
}

/**
 *
 */
int kvstore_replay_del(kvstore* store, int key) {
    if (!store) return KVSTORE_ERR_NULL;

    if (!kvstore_state_allow(store->state, KVSTORE_OP_REPLAY))
        return KVSTORE_ERR_INTERNAL_STATE;

    return kvstore_apply_del(store, key);
}

/**
 * kvstore_replay_log - 日志重放（WAL Replay / Recovery）
 *
 * 功能：
 *  - 从 WAL 文件中顺序读取历史操作（PUT / DEL）
 *  - 校验每一条日志记录完整性（CRC）
 *  - 将合法操作重新 apply 到内存中的 B+ Tree
 *
 *
 * 设计约束：
 *  - 不写 WAL (否则会造成日志膨胀)
 *  - 不走 Public API (绕过 readonly / 权限检查)
 *  - 只调用 apply_internal 系列函数
 *
 * 修改原则（v4）
 *  - replay 不感知系统状态
 *  - replay 不做状态回滚
 *  - replay 假设：调用者已经准备好一切
 */
static int kvstore_replay_log(kvstore* store) {
    if (!store || !store->tree || !store->log_fp) RETURN_ERR(KVSTORE_ERR_NULL);

    // 防御性断言
    if (store->state != KVSTORE_STATE_RECOVERING) {
        RETURN_ERR(KVSTORE_ERR_INTERNAL);
    }

    char line[256];
    int rc = KVSTORE_OK;

    // 1. 初始化统计结构体
    replay_stats stats = {0};

    rewind(store->log_fp);

    // 2. 读取并校验 日志头
    if (!fgets(line, sizeof(line), store->log_fp)) {
        // 空日志是合法（新建数据库）
        goto out;
    }

    if (kvstore_log_header(line) != KVSTORE_OK) {
        stats.corrupted++;
        rc = KVSTORE_ERR_WAL_CORRUPTED;
        goto out;
    }

    // 3. 逐行重放日志体
    while (fgets(line, sizeof(line), store->log_fp)) {
        /* 跳过空行 */
        if (line[0] == '\n' || line[0] == '\r' || line[0] == ' ')
            continue;

        /* 去掉末尾换行（\n 或 \r\n） */
        line[strcspn(line, "\r\n")] = '\0';

        /* 拆分 payload | crc */
        char* sep = strchr(line, '|');
        if (!sep) {
            // 容忍尾行被破坏，不报错，直接当作读完
            stats.skipped++;
            rc = KVSTORE_OK;
            break;
        }

        *sep = '\0';
        char* crc_str = sep + 1;

        /* CRC 校验（只校验 payload） */
        if (!kvstore_crc_check(line, crc_str)) {
            stats.corrupted++;

            if (feof(store->log_fp)) {
                // 如果是最后一行损坏，可能是断电，容忍它
                rc = KVSTORE_OK;
            } else {
                // 如果是中间行损坏，说明文件逻辑断裂，必须停止
                rc = KVSTORE_ERR_WAL_CORRUPTED;
                fprintf(stderr, "[REPLAY] 中间数据损坏! Payload: [%s]\n", line);
            }
            break;
        }

        /* 根据日志内容 apply 到内存 */
        int key;
        long val;
        int ret;

        if (sscanf(line, "PUT %d %ld", &key, &val) == 2) {
            ret = kvstore_replay_put(store, key, val);
        } else if (sscanf(line, "DEL %d", &key) == 1) {
            ret = kvstore_replay_del(store, key);
        } else {
            // 解析失败（比如格式写错了），计入 skipped
            stats.skipped++;
            continue;

            // rc = KVSTORE_ERR_INTERNAL;
            // break;
        }

        if (ret == KVSTORE_OK) {
            stats.applied++;
        } else {
            rc = ret;
            break;
        }
    }
    // 在退出前打印统计信息
    int total = stats.applied + stats.skipped + stats.corrupted;
out:
    
    printf(
        "[RECOVERY] applied=%d skipped=%d corrupted=%d success_rate=%.2f%%\n",
        stats.applied,
        stats.skipped,
        stats.corrupted,
        total ? (100.0 * stats.applied / total) : 100.0);

    if (rc != KVSTORE_OK) {
        return kvstore_fatal(store, rc);
    }

    return KVSTORE_OK;
}

/**
 * kvstore_strerror - 将 KVstore 错误码转换为可读字符串
 *
 * 设计说明：
 *  - 纯函数（无副作用，无状态），不进行任何 I/O 或日志输出
 *  - 返回的字符串为静态只读字符串，调用方不得修改
 *
 * 使用场景：
 *  - 调试输出
 *  - 日志系统
 *  - 对外 API 错误提示
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
        case KVSTORE_ERR_WAL_CORRUPTED:
            return "data corrupted";
        default:
            return "unknown error";
    }
}

/**
 * kvstore_debug_set_mode - 手动强制切换数据库运行模式
 *
 *  - 使用 #ifdef DEBUG 包裹，确保此“危险”函数不会出现在正式发布的版本
 *
 * [执行链条]
 *  1. 强制设定模式 -> 2. 自动推导状态 -> 3. 立即应用权限刷新。
 */
void kvstore_debug_set_mode(kvstore* store, kvstore_mode_t mode) {
#ifdef DEBUG

    if (!store) return;

    kvstore_set_mode(store, mode);

    kvstore_state_t state = kvstore_state_mode(mode);
    kvstore_set_state(store, state);

    // DEBUG 不允许手动 apply
    kvstore_apply_state(store);

#endif
}

/**
 * kvstore_enter_compaction - 准备进入压缩阶段：将系统切换至只读保护状态
 *
 * 设计意图：
 *  - compaction 是一个耗时的“重量级操作”。在执行期间，必须通过将 state 切换为
 *    KVSTORE_STATE_READONLY(只读) 来拦截所有并发的写请求（PUT / DEL）
 *
 *  - 只有处于 READY(正常服务)的系统才能发起压缩。此函数确保压缩操作不会在
 *    恢复中（RECOVERY）或已损坏（CURRUPTED）的情况下被非法触发
 */
static int kvstore_enter_compaction(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    if (store->state != KVSTORE_STATE_READY)
        return KVSTORE_ERR_READONLY;

    store->state = KVSTORE_STATE_READONLY;
    kvstore_apply_state(store);

    return KVSTORE_OK;
}

/**
 * kvstore_exit_compaction - 退出压缩阶段：恢复系统压缩前的运行状态
 *
 * 设计说明：
 *  - 压缩完成后，通过传入 prev 参数，可以将系统恢复到他所前的状态
 *
 *  - 无论压缩成功还是失败，都必须调用此函数来解锁系统权限，否则系统将永远停留在只读模式
 *
 *  这里通常是写锁的地方 ！
 */
static void kvstore_exit_compaction(kvstore* store, kvstore_state_t prev) {
    store->state = prev;
    kvstore_apply_state(store);
}

/**
 * kvstore_exec_write
 *  仅提供 public write API 使用
 *  - apply 到内存
 *  - 运行期维护
 *
设计意图：
    kvstore_put ─┐
                ├─ wal
                ├─ apply
                └─ runtime
    kvstore_del ─┘

    kvstore_replay_* ──→ apply only
 */
static int kvstore_exec_write(kvstore* store, kvstore_op_t op, int key, int value) {
    int ret;

    /* 1. 基础合法性  */
    if (!store)
        return KVSTORE_ERR_NULL;

    /* 2.状态检查：只能在 READY 写 */
    if (!kvstore_state_allow(store->state, op)) {
        // printf("DEBUG: 状态拦截！当前 state=%d, 操作=%d\n", store->state, op);
        return KVSTORE_ERR_INTERNAL_STATE;
    }

    /* 3. 先写 WAL */
    switch (op) {
        case KVSTORE_OP_PUT:
            ret = kvstore_log_put(store, key, value);
            break;
        case KVSTORE_OP_DEL:
            ret = kvstore_log_del(store, key);
            break;

        default:
            return KVSTORE_ERR_INTERNAL;
    }

    if (ret != KVSTORE_OK)
        return kvstore_fatal(store, ret);

    /* 4. 再 apply 到内存 */
    switch (op) {
        case KVSTORE_OP_PUT:
            ret = kvstore_apply_put(store, key, value);
            break;
        case KVSTORE_OP_DEL:
            ret = kvstore_apply_del(store, key);
            break;

        default:
            return KVSTORE_ERR_INTERNAL;
    }

    if (ret != KVSTORE_OK) {
        return kvstore_fatal(store, ret);
    }

    /* 5. 运行态维护 */
    store->ops_count++;
    kvstore_maybe_compact(store);

    return KVSTORE_OK;
}

/**
 * kvstore_fatalb
 *  - 这个错误认定为不可恢复
 */
static int kvstore_fatal(kvstore* store, int err) {
    if (!store)
        return err;

    kvstore_enter_failed(store);
    return err;
}

/**
 *
 */
static int kvstore_transit_state(kvstore* store, kvstore_state_t next) {
    if (!store)
        return KVSTORE_ERR_NULL;

    kvstore_state_t prev = store->state;

    /* 1. 状态转移合法性检查 */
    switch (prev) {
        case KVSTORE_STATE_INIT:
            if (next != KVSTORE_STATE_RECOVERING)
                return KVSTORE_ERR_INTERNAL_STATE;
            break;

        case KVSTORE_STATE_RECOVERING:
            if (next != KVSTORE_STATE_READY && next != KVSTORE_STATE_CORRUPTED)
                return KVSTORE_ERR_INTERNAL_STATE;
            break;

        case KVSTORE_STATE_READY:
            if (next != KVSTORE_STATE_READY && next != KVSTORE_STATE_CORRUPTED)
                return KVSTORE_ERR_INTERNAL_STATE;
            break;

        case KVSTORE_STATE_CLOSING:
            return KVSTORE_ERR_INTERNAL_STATE;

        case KVSTORE_STATE_CORRUPTED:
            return KVSTORE_ERR_INTERNAL_STATE;

        default:
            return KVSTORE_ERR_INTERNAL_STATE;
    }

    /* 2. 真正的修改状态 */
    store->state = next;

    /* 3. 派生状态同步（readonly / mode）*/
    kvstore_apply_state(store);

    return KVSTORE_OK;
}

/**
 * 决策矩阵 - 唯一判定函数
 *
| state ↓ / op → | REPLAY | PUT | DEL | GET | CLOSE | DESTROY |
| -------------- | ------ | --- | --- | --- | ----- | ------- |
| INIT           | ❌      | ❌   | ❌   | ❌   | ❌     | ❌       |
| RECOVERING     | ✅      | ❌   | ❌   | ❌*  | ❌     | ❌       |
| READY          | ❌      | ✅   | ✅   | ✅   | ✅     | ❌       |
| READONLY       | ❌      | ❌   | ❌   | ✅   | ✅     | ❌       |
| CLOSING        | ❌      | ❌   | ❌   | ❌   | ❌     | ❌       |
| CORRUPTED      | ❌      | ❌   | ❌   | ❌   | ❌     | ✅       |
 */
static int kvstore_state_allow(kvstore_state_t state, kvstore_op_t op) {
    switch (op) {
        /* ========== 写操作 ========== */
        case KVSTORE_OP_PUT:
        case KVSTORE_OP_DEL:
            return state == KVSTORE_STATE_READY;

        /* ========== 读操作 ========== */
        case KVSTORE_OP_GET:
            return state == KVSTORE_STATE_READY || state == KVSTORE_STATE_READONLY;

        /* ========== 系统内部操作 ========== */
        case KVSTORE_OP_REPLAY:
            return state == KVSTORE_STATE_RECOVERING;

        case KVSTORE_OP_CLOSE:
        case KVSTORE_OP_DESTROY:
            return state != KVSTORE_STATE_CLOSING;

        default:
            return 0;
    }
}

/**
 *
 * kvstore_open is the ONLY valid constructor.
 * kvstore_create is allocator only (no IO, no replay).
 *
    kvstore_open()
        ↓
    kvstore_recover()   // WAL / snapshot replay
        ↓
    kvstore_enter_ready()
        ↓
    （对外 put / del / get）
        ↓
    kvstore_close()
 */
kvstore* kvstore_open(const char* wal_path) {
    kvstore* store = NULL;
    int ret;

    /* 1. alloc + basic init */
    store = kvstore_create();
    if (!store)
        return NULL;

    /* 2. open WAL (I/O only) */
    ret = kvstore_log_open(store, wal_path);
    if (ret != KVSTORE_OK)
        goto fail;

    /* load snapshot（允许不存在） */
    ret = kvstore_load_snapshot(store);
    if (ret != KVSTORE_OK)
        goto fail;

    /* 3. enter recovering */
    ret = kvstore_enter_recovering(store);
    if (ret != KVSTORE_OK)
        goto fail;

    /* 4. replay WAL */
    ret = kvstore_replay_log(store);
    if (ret != KVSTORE_OK)
        goto fail;

    /* 5. enter ready */
    ret = kvstore_enter_ready(store);
    if (ret != KVSTORE_OK)
        goto fail;

    return store;

fail:
    kvstore_close(store);
    return NULL;
}

int kvstore_recover(kvstore* store) {
    int ret;

    if (!store)
        return KVSTORE_ERR_NULL;

    /* 1. 状态校验 */
    if (!kvstore_state_allow(store->state, KVSTORE_OP_REPLAY))
        return KVSTORE_ERR_INTERNAL_STATE;

    /* 2. replay WAL */
    ret = kvstore_replay_log(store);
    if (ret != KVSTORE_OK) {
        kvstore_enter_failed(store);
        return ret;
    }

    /* 3. 恢复完成，进入 READY */
    return kvstore_enter_ready(store);
}

int kvstore_close(kvstore* store) {
    if (!store)
        return KVSTORE_ERR_NULL;

    /* 已经 close 过，幂等 */
    if (store->state == KVSTORE_STATE_CLOSED)
        return KVSTORE_OK;

    /* 1. 状态切换：拒绝新请求 */
    kvstore_transit_state(store, KVSTORE_STATE_CLOSING);

    /* 2. 刷 WAL + 关闭文件 */
    kvstore_log_close(store);  // 内部 fclose + fsync

    /* 3. 状态锁定 */
    kvstore_transit_state(store, KVSTORE_STATE_CLOSED);

    return KVSTORE_OK;
}

int kvstore_log_close(kvstore* store) {
    if (!store)
        return KVSTORE_ERR_NULL;

    if (!store->log_fp)
        return KVSTORE_OK;

    /* 1. 刷盘，保证 WAL 落地 */
    fflush(store->log_fp);

    /* 2. 关闭文件 */
    fclose(store->log_fp);
    store->log_fp = NULL;

    /* 3. 清理元信息（防止悬空） */
    store->log_size = 0;

    return KVSTORE_OK;
}

/**
 * kvstore_log_open
 *
 * 功能：
 *  - 打开或者创建 WAL file
 *  - 确保 log header 存在
 *  - 为仅可追加写入准备文件
 *
 *  NOTE:
 *  - does NOT replay log
 *  - does NOT change kvstore state
 */
int kvstore_log_open(kvstore* store, const char* wal_path) {
    if (!store || !wal_path)
        return KVSTORE_ERR_NULL;

    FILE* fp = fopen(wal_path, "a+");
    if (!fp)
        return KVSTORE_ERR_IO;

    store->log_fp = fp;
    snprintf(store->log_path, sizeof(store->log_path), "%s", wal_path);

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    store->log_size = size;

    if (size == 0) {
        fprintf(fp, "%s\n", KVSTORE_LOG_VERSION);
        fflush(fp);
        int fd = fileno(fp);
        if (fd >= 0) fsync(fd);
        store->log_size = ftell(fp);
    }

    /* 为 replay 做准备 */
    fseek(fp, 0, SEEK_SET);

    return KVSTORE_OK;
}