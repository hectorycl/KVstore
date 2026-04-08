#include "kvstore.h"

#include <assert.h>
#include <pthread.h>
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
    pthread_mutex_t log_lock;  // 保护日志文件并发写入

    kvstore_state_t state; /* v4 新增 */
    kvstore_mode_t mode;   /* v3, 降级为实现细节 */
    int readonly;          // replay / recovery 期间只读状态，禁止写

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

/* =========================================================
 * Recovery / Startup
 * ========================================================= */
static int kvstore_replay_log(kvstore* store);
static int kvstore_load_snapshot(kvstore* store);
static int kvstore_log_header(const char* line);

/**
 * =========================================================
 * WAL Logging 写入层
 * 架构边界，不碰 B+ 树
 * ========================================================= */
static int kvstore_log_put(kvstore* store, int key, long value);
static int kvstore_log_del(kvstore* store, int key);

/**
 * =========================================================
 * Apply (Memory Mutation)
 * （只修改内存结构，不涉及IO）
 * =========================================================*/
static int kvstore_apply_put(kvstore* store, int key, long value);
static int kvstore_apply_del(kvstore* store, int key);

static int kvstore_apply_put_internal(bptree* tree, int key, long value);
static int kvstore_apply_del_internal(bptree* tree, int key);

/**
 * =========================================================
 * Atomic Execution （原子执行）
 *========================================================= */
static int kvstore_replay_put(kvstore* store, int key, long value);
static int kvstore_replay_del(kvstore* store, int key);

static int kvstore_exec_write(kvstore* store, kvstore_op_t op, int key, int value);
static int kvstore_state_allow(kvstore_state_t state, kvstore_op_t op);

/**
 * =========================================================
 * Snapshot / Compaction
 * (存储维护)
 * ========================================================= */
static void kvstore_maybe_compact(kvstore* store);

int kvstore_compact(kvstore* store);
static int kvstore_compact_internal(kvstore* store);

static int compact_write_cb(int key, long value, void* arg);
static int snapshot_write_cb(int key, long value, void* arg);

/**
 * =========================================================
 * State Machine
 *  - 状态机（System State Machine）
 *  -- 所有锁策略的调度中心
 * ========================================================= */
static int kvstore_transit_state(kvstore* store, kvstore_state_t next);
static int kvstore_enter_recovering(kvstore* store);
static int kvstore_enter_ready(kvstore* store);
static int kvstore_enter_failed(kvstore* store);
static int kvstore_enter_compaction(kvstore* store);
static void kvstore_exit_compaction(kvstore* store, kvstore_state_t prev);
static void kvstore_apply_state(kvstore* store);

/**
 * =========================================================
 * Error Handling
 *  - Fatal / Error / Debug (横切功能)
 * ========================================================= */
static int kvstore_fatal(kvstore* store, int err);
const char* kvstore_strerror(int err);

/**
 * =========================================================
 * 底层赋值函数
 * ========================================================= */
static int kvstore_crc_check(const char* payload, const char* crc_str);

// ==============  实现  ==================

/**
 * =========================================================
 * 1️⃣ Public API — 生命周期（最顶层入口）
 * ========================================================= */

/**
 * 初始化 kvstore 实例对象（内存分配阶段）
 *
 * @note 该函数仅在内存中创建结构。此时系统处于 KVSTORE_STATE_INIT 状态，
 * 在调用 kvstore_open() 或 kvstore_recover() 之前，无法进行数据操作。
 */
kvstore* kvstore_create() {
    // 1. 结构体内存分配
    kvstore* store = malloc(sizeof(*store));
    if (!store) return NULL;

    // 2. 核心索引初始化（apply 层的物质基础）
    // 锁的初始化细节被“封装”在 bptree_create 内部了
    store->tree = bptree_create();
    if (!store->tree) {
        free(store);  // 异常处理：防止内存泄漏，索引创建失败必须释放外层容器
        return NULL;
    }

    // 3. 状态机与元数据初始化
    store->state = KVSTORE_STATE_INIT;
    store->log_fp = NULL;
    store->log_size = 0;
    store->ops_count = 0;
    store->mode = KVSTORE_MODE_NORMAL;
    store->readonly = 0;  // 默认可写

    // 初始化日志锁
    pthread_mutex_init(&store->log_lock, NULL);

    return store;
}

/**
 * kvstore_destroy - 销毁并彻底回收 kvstore 实例的所有资源
 *
 * 功能：
 *  1. 检查并强制关闭尚未停机的日志与状态
 *  2. 调用索引引擎的销毁接口，释放复杂的内存树结构
 *  3. 释放主控制块内存
 *
 *  - 支持 NULL 传入，具有幂等性
 *  - 一旦调用此函数，原指针 store 将失效，不能再次解引用
 */
void kvstore_destroy(kvstore* store) {
    if (!store) return;

    /**
     *  1. 状态机与持久化补齐：
     * 确保在销毁内存前，所有文件 IO (Logging 层已安全关闭) */
    if (store->state != KVSTORE_STATE_CLOSED) {
        kvstore_close(store);
    }

    /**
     * 2. 索引层清理：
     * 先销毁子结构（Tree）,在销毁父结构（Store）*/
    if (store->tree) {
        bptree_destroy(store->tree);
        store->tree = NULL;
    }

    /* 3. 最终释放 store 本体，将控制块占用的堆空间还给 OS */
    free(store);
}

/**
 * kvstore_open - 系统唯一合法构造函数
 *
 * 负责编排整个 KVstore 的启动流程，遵循“全或无”原则，确保返回的指针处于
 *  READY 状态，或者在失败时彻底清理所有资源
 *
 * @layers:
 * 1. Lifecycle: 分配内存对象。
 * 2. Logging: 挂载磁盘文件。
 * 3. Recovery: 恢复快照(Snapshot)并重放(Replay)增量日志
 * 4. State Machine: 管理从 INIT -> RECOVERY -> READY 的状态演变
 *
 * wal_path - 日志文件的路径（若文件不存在则创建）
 *
 */
kvstore* kvstore_open(const char* wal_path) {
    kvstore* store = NULL;
    int ret;

    /* 1. [Lifecycle] 初始化内存结构  */
    store = kvstore_create();
    if (!store) return NULL;

    /* 2. [Logging] 绑定磁盘持久化层 */
    ret = kvstore_log_open(store, wal_path);
    if (ret != KVSTORE_OK) goto fail;

    /* 3. [Recovery] 加载最近的一次数据快照（允许不存在） */
    ret = kvstore_load_snapshot(store);
    if (ret != KVSTORE_OK) goto fail;

    /* 4. [State Machine] 锁定状态为恢复中，拦截并发访问量 */
    ret = kvstore_enter_recovering(store);
    if (ret != KVSTORE_OK) goto fail;

    /* 5. [Recovery] 重放 WAL 增量操作，对其内存与磁盘 */
    ret = kvstore_replay_log(store);
    if (ret != KVSTORE_OK) goto fail;

    /* 6. [State Machine] 完成恢复，正式开放读写权限 */
    ret = kvstore_enter_ready(store);
    if (ret != KVSTORE_OK) goto fail;

    return store;

fail:
    /* [Error/Cleanup] 统一资源回收 */
    kvstore_close(store);
    return NULL;
}

/**
 * kvstore_recover - 触发数据恢复流程，对齐内存与磁盘状态
 *  - 拦截异常：kvstore_state_allow
 *  - 异常降级：一旦恢复逻辑（replay_log）失败，立即调用 enter_failed 锁定系统。
 */
int kvstore_recover(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    /* 1. 状态校验 */
    if (!kvstore_state_allow(store->state, KVSTORE_OP_REPLAY))
        return KVSTORE_ERR_INTERNAL_STATE;

    /* 2. replay WAL - 执行核心重放*/
    // 遍历 WAL 文件，调用 apply 层 修改内存
    int ret = kvstore_replay_log(store);
    if (ret != KVSTORE_OK) {
        kvstore_enter_failed(store);
        return ret;
    }

    /* 3. 恢复完成，进入 READY */
    return kvstore_enter_ready(store);
}

/**
 * kvstore_log_open - 开启或初始化 WAL 日志系统
 *
 * @details
 * 本函数负责将内存实例与磁盘文件正式对接。不仅负责文件句柄的分配，
 * 还承担了“格式化”新日志文件的任务。
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
    // 1. 基础参数合法性校验
    if (!store || !wal_path)
        return KVSTORE_ERR_NULL;

    // 2. 以读写追加模式开启物理文件
    FILE* fp = fopen(wal_path, "a+");
    if (!fp) return KVSTORE_ERR_IO;

    // 3. 绑定文件状态至 store 控制块
    store->log_fp = fp;
    snprintf(store->log_path, sizeof(store->log_path), "%s", wal_path);

    // 4. 探测文件物理大小，用于触发 Compaction 逻辑
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    store->log_size = size;

    // 5. 关键：初始化空日志的 Header
    if (size == 0) {
        fprintf(fp, "%s\n", KVSTORE_LOG_VERSION);
        fflush(fp);
        int fd = fileno(fp);

        // 持久化核心，强制 OS 执行物理刷盘，确保 Header 落地
        if (fd >= 0) fsync(fd);

        store->log_size = ftell(fp);
    }

    // 6. 重置偏移量，为接下来的 Recovery/Replay 流程做准备
    fseek(fp, 0, SEEK_SET);

    return KVSTORE_OK;
}

/**
 * kvstore_close - 安全关闭 WAL 日志系统
 *
 * @details
 * 该函数负责断开内存实例与物理磁盘日志的连接。不仅仅是释放句柄，更是
 * 确保所有尚未写入磁盘的数据得到最后的妥善处理
 *
 *
 */
int kvstore_log_close(kvstore* store) {
    // 1. 基础校验：防止对空对象进行操作
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 幂等性支持：若已关闭则直接跳过
    if (!store->log_fp)
        return KVSTORE_OK;

    /**
     * 3. 数据完整性保护：
     * 将 C 库缓冲区中的最后字节同步至 OS 内核 */
    fflush(store->log_fp);

    /**
     * 4. 句柄释放：
     * 归还操作系统资源，并立即将指针归零以防逻辑层重复利用 */
    fclose(store->log_fp);
    store->log_fp = NULL;  // 防止野指针

    /**
     * 5. 状态同步：
     * 清除关于磁盘文件的元数据记录，确保内存状态与 IO 状态严格一致*/
    store->log_size = 0;

    return KVSTORE_OK;
}

/**
 * kvstore_close - 逻辑停机：安全停止 KVStore 的运行
 *
 * @details
 * 该函数实现了从“在线”到“离线”的平滑过渡。它不仅关闭物理文件，更重要的是通过
 * 状态机变更，在关闭期间建立起逻辑防火墙
 *
 * [设计逻辑]
 * 1. 采用中间态（CLOSING）,确保在执行物理 I/O 操作时，
 *   外部新的业务请求会被拦截，避免数据在关闭瞬间产生的不一致
 * 2. 委托 Logging 层完成文件句柄的释放和缓冲区刷新
 * 3. 此函数仅执行逻辑关闭，并不释放 store 指针内存，若要彻底销毁，
 *   后续要调用 kvstore_destroy()
 *
 */
int kvstore_close(kvstore* store) {
    /* 1. 安全边界检查 */
    if (!store) return KVSTORE_ERR_NULL;

    /* 2. 幂等性支持：避免重复关闭导致的系统逻辑混乱 */
    if (store->state == KVSTORE_STATE_CLOSED)
        return KVSTORE_OK;

    /* 3. [状态机] 第一阶段：设置“正在关闭屏障”，拒绝新的 Put/Del 操作 */
    kvstore_transit_state(store, KVSTORE_STATE_CLOSING);

    /* 4. [状态机] 物理操作：将缓冲区数据推向磁盘并关闭文件描述符 fd */
    kvstore_log_close(store);  // 内部 fclose + fsync

    /* 3. [状态机] 第二阶段：标记实例已进入静默态 */
    kvstore_transit_state(store, KVSTORE_STATE_CLOSED);

    // 销毁锁
    pthread_mutex_destroy(&store->log_lock);

    return KVSTORE_OK;
}

/**
 * =========================================================
 * 2️⃣Public API — 基本操作
 * ========================================================= */

/**
 * kvstore_put - 向存储引擎写入或更新一个键值对
 *
 * @details
 * 该函数是“逻辑入口”。不做对数据进行实际的 B+ 树插入，而是将操作封装后，
 * 转发至 Execution 的原子写入路径
 *
 *
 * [关键不变量]
 *  - 所有用户写操作，必须先成功写 WAL，才能修改内存
 *  - Apply Layer（内部 put_internal） 只负责“状态变更”，不做权限和持久化判断
 *
 * exec 只给在线写用，replay 只做 apply
 */
int kvstore_put(kvstore* store, int key, long value) {
    // 1. 基础安全校验
    if (!store || !store->tree) return KVSTORE_ERR_NULL;

    // 2. 原子执行
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
    // 1. 参数合法性校验
    if (!store || !store->tree) return KVSTORE_ERR_NULL;

    // 2. 执行写入逻辑（DEL 也是一种写操作）
    return kvstore_exec_write(store, KVSTORE_OP_DEL, key, 0);
}

/**
 * kvstore_create_snapshot - 创建内存状态的快照
 *
 * @details
 *  - 采用 tmp 文件 + rename 的原子替换策略
 *  - 使用 B+ Tree scan + callback 解耦策略 （还要重点学习）
 *  - 显示 flush + fsync 保证落盘
 */
int kvstore_create_snapshot(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    // 先写 tmp, 成功后再原子替换
    const char* tmp = "data.snapshot.tmp";
    const char* snap = "data.snapshot";

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
 * =========================================================
 * 3️⃣ Public API — 高级接口
 * ========================================================= */

/**
 * kvstore_search - 对外查询接口（存储引擎中检索指定键的值）
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
    // 1. 防御性检查
    if (!store || !store->tree) return KVSTORE_ERR_NULL;

    // 2. 状态校验 (确保不是 CLOSED 或 CORRUPTED)
    if (!kvstore_state_allow(store->state, KVSTORE_OP_GET)) {
        return KVSTORE_ERR_INTERNAL_STATE;
    }

    // 3. 加锁并执行搜索
    pthread_mutex_lock(&store->log_lock);
    int ret = bptree_search(store->tree, key, value);
    pthread_mutex_unlock(&store->log_lock);

    return ret;
}

/**
 * kvstore_compact - 统筹日志压缩流程，负责运行态的平滑切换
 *
 * @details
 *  - 通过 enter/exit 函数实现逻辑锁，确保快照期间把内存索引不被修改
 *
 * 设计模式：包裹模式 （保存-修改-还原）
 *  1. 现场保护：在执行前通过 prev 变量备份当前的系统状态
 *  2. 状态锁定：调用 enter_compaction 切换为只读，防止压缩期间数据被篡改
 *  3. 核心执行：将压缩工作委派给内部函数 compact_internal 处理
 *  4. 现场恢复：无论压缩成功与否，最终都通过 exit_compaction 恢复之前的系统状态
 *
 *  不会导致数据库永久锁死在只读模式，系统几倍自我恢复能力
 *
 * [分层好处]
 *  - kvstore_compact 关注的是流程控制；kvstore_compact_internal 关注的是文件 IO
 *
 * 体现了面向切面编程（AOP）的思想
 */
int kvstore_compact(kvstore* store) {
    int ret;
    kvstore_state_t prev;

    // 1. 安全性检查
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 状态保存
    prev = store->state;

    // 3. 前置处理：锁定状态（此时禁止写入）
    ret = kvstore_enter_compaction(store);
    if (ret != KVSTORE_OK)
        return ret;

    // 4. 核心执行 - 执行具体的快照生成和 WAL 阶段逻辑
    ret = kvstore_compact_internal(store);

    // 5. 后置处理：解锁恢复
    // 无论压缩成功与否，都必须调用此函数将状态还原（prev）
    kvstore_exit_compaction(store, prev);

    return ret;
}

/**
 * =========================================================
 * 4️⃣ Debug / Utility API (底层辅助函数)
 * ========================================================= */

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
            return "null pointer";  // NULL 指针的拦截
        case KVSTORE_ERR_INTERNAL:
            return "internal error";  // 状态机非法转移或者逻辑错误
        case KVSTORE_ERR_IO:
            return "io error";  // 磁盘满、权限不足
        case KVSTORE_ERR_READONLY:
            return "read-only mode";  // 系统处于受限状态（如备份中）
        case KVSTORE_ERR_NOT_FOUND:
            return "key not found";
        case KVSTORE_ERR_WAL_CORRUPTED:
            return "data corrupted";
        default:
            return "unknown error";
    }
}

// CRC 函数（简单版）- 循环冗余校验
/**
 * crc32 - 计算字符串的 32 位循坏冗余码（CRC32）
 */
uint32_t crc32(const char* s) {
    // 1. 初始化寄存器
    uint32_t crc = 0xFFFFFFFF;

    // 2. 逐字节处理字符串
    // 遍历输入字符串，直到遇到 '\0'
    while (*s) {
        crc ^= (unsigned char)*s++;
        for (int i = 0; i < 8; i++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }

    return ~crc;
}

/**
 * kvstore_crc_check - 校验单行日志的完整性
 *  - 成功返回 1， 失败返回 0
 * 注意：此函数会修改 line 字符串（就地切割）
 *      是确保存储引擎在崩溃恢复时不载入脏数据的最后屏障
 */
static int kvstore_crc_check(const char* payload, const char* crc_str) {
    // 1. 基础校验
    if (!payload || !crc_str) return 0;

    // 2. 字符串转数字
    char* end = NULL;
    uint32_t crc_stored = (uint32_t)strtoul(crc_str, &end, 10);

    /* 3. 健壮性检查：校验 CRC 字段本身是否损坏  */
    if (end == crc_str || *end != '\0')
        return 0;

    uint32_t crc_calc = crc32(payload);
    return crc_calc == crc_stored;
}

/**
 * =========================================================
 * 5️⃣ Recovery / Startup 内部实现
 * ========================================================= */
/**
 * kvstore_replay_log - 重放 WAL 日志以重建内存索引
 *
 * 功能：
 *  - 从 WAL 文件中顺序读取历史操作（PUT / DEL）
 *  - 校验每一条日志记录完整性（CRC）
 *  - 将合法操作重新 apply 到内存中的 B+ Tree
 *
 * 设计约束：
 *  - 不写 WAL (否则会造成日志膨胀)
 *  - 不走 Public API (绕过 readonly / 权限检查)
 *  - 只调用 apply_internal 系列函数
 */
static int kvstore_replay_log(kvstore* store) {
    // 1. 环境检查
    if (!store || !store->tree || !store->log_fp) RETURN_ERR(KVSTORE_ERR_NULL);

    // 2. 状态机防御
    if (store->state != KVSTORE_STATE_RECOVERING) {
        RETURN_ERR(KVSTORE_ERR_INTERNAL);
    }

    char line[256];
    int rc = KVSTORE_OK;
    replay_stats stats = {0};

    // 3. 重置指针：从日志开头开始读
    rewind(store->log_fp);

    // 4. 校验日志头(Header Check)
    if (!fgets(line, sizeof(line), store->log_fp)) {
        goto out;  // 空文件是合法的（刚刚初始化）
    }

    if (kvstore_log_header(line) != KVSTORE_OK) {
        stats.corrupted++;
        rc = KVSTORE_ERR_WAL_CORRUPTED;
        goto out;
    }

    // 5. 循环重放日志体
    while (fgets(line, sizeof(line), store->log_fp)) {
        /* A. 清洗数据 */
        if (line[0] == '\n' || line[0] == '\r' || line[0] == ' ') continue;
        line[strcspn(line, "\r\n")] = '\0';  // 统一去掉换行符

        /* B. 拆分 payload | crc */
        char* sep = strchr(line, '|');
        if (!sep) {
            // 容忍尾行被破坏，不报错，直接当作读完
            stats.skipped++;
            rc = KVSTORE_OK;
            break;
        }

        *sep = '\0';
        char* crc_str = sep + 1;

        /* C. CRC 数据完整性校验 */
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

        /* D. 解析动作并 Apply 到内存 */
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
    // int total = stats.applied + stats.skipped + stats.corrupted;

    // 6. 统计收尾
out:

    // printf(
    //     "[RECOVERY] applied=%d skipped=%d corrupted=%d success_rate=%.2f%%\n",
    //     stats.applied,
    //     stats.skipped,
    //     stats.corrupted,
    //     total ? (100.0 * stats.applied / total) : 100.0);

    if (rc != KVSTORE_OK) {
        return kvstore_fatal(store, rc);
    }

    return KVSTORE_OK;
}

/**
 * kvstore_load_snap - 加载磁盘快照，以快速恢复基础数据
 *
 * @details
 * 该函数是系统启动的第一步。将磁盘上的全量固化数据载入内存，
 * 使得后续的 WAL 重放只需处理自“自上次快照以来的增量”，从而大幅缩短启动时间。
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
    // 1. 基础校验
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 尝试开启快照文件
    FILE* fp = fopen("data.snapshot", "r");
    if (!fp) {
        // snapshot 不存在，允许慢启动（会跳过快照）
        return KVSTORE_OK;
    }

    char line[256];
    int key;
    long value;

    // 3. 全量数据注入循环
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

    // 5.资源回收
    fclose(fp);
    return KVSTORE_OK;
}

/**
 * kvstore_log_header - 验证 WAL 日志文件的元数据合法性
 *
 * 这是启动恢复的第一步，如果版本号对不上，吧必须立即停止
 */
static int kvstore_log_header(const char* line) {
    // 1. 字符串前缀比对
    if (strncmp(line, KVSTORE_LOG_VERSION, strlen(KVSTORE_LOG_VERSION)) != 0) {
        // 2.致命错误输入
        fprintf(stderr, "Invalid log version. Expected: %s\n", KVSTORE_LOG_VERSION);
        return KVSTORE_ERR_WAL_CORRUPTED;
    }

    // 3. 通过验证
    return KVSTORE_OK;
}

/**
 * =========================================================
 * 6️⃣ WAL 写入
 * ========================================================= */

/**
 * 日志持久化（write-ahead Logging）
 * - 负责将 PUT 操作以追加模式写入磁盘
 * - 格式标准: "PUT <key> <value>|<crc32>\n"
 *
 * snprintf(): 将变量格式化为字符串，并安全写入缓冲区
 *
 */
static int kvstore_log_put(kvstore* store, int key, long value) {
    // [1] 参数与状态校验
    if (!store || !store->log_fp) return KVSTORE_ERR_NULL;

    char payload[128];
    char final_line[160];

    // [2] 构造有效载荷（payload）并计算校验和
    // 使用 snprintf 确保缓冲区安全
    int payload_len = snprintf(payload, sizeof(payload), "PUT %d %ld", key, value);
    if (payload_len < 0 || payload_len >= (int)sizeof(payload)) {
        return KVSTORE_ERR_INTERNAL;
    }

    uint32_t checksum = crc32(payload);

    // [3] 构造最终行内容
    // 显示将数据与校验和拼接，确保物理写入时是一次完整的 I/O 操作
    int total_len = snprintf(final_line, sizeof(final_line), "%s|%u\n", payload, checksum);
    if (total_len < 0 || total_len >= (int)sizeof(final_line)) {
        return KVSTORE_ERR_INTERNAL;
    }

    // [4] 执行物理追加写入
    // fputs 相比 fprintf 更高效，因为它不需要再次解析格式化占位符
    if (fflush(store->log_fp) != 0) {
        return KVSTORE_ERR_IO;
    }

    // [6] 更新统计信息
    // log_size 用于触发后续的自动压缩 (Compaction)
    store->log_size += total_len;

    return KVSTORE_OK;
}

/**
 * kvstore_log_del - 将删除操作持久化到 WAL
 *
 * 删除在磁盘上不是物理抹除，而是追加一条 DEL 指令
 *
 * 日志格式：
 *      DEL <key>|<crc32>\n
 *
 */
static int kvstore_log_del(kvstore* store, int key) {
    // 1. 安全边界检查
    if (!store->log_fp) return KVSTORE_ERR_NULL;

    // 2. 序列化
    char buf[128];
    snprintf(buf, sizeof(buf), "DEL %d", key);

    // 3. 生成数据指纹
    uint32_t crc = crc32(buf);

    // 4. 物理写入
    int bytes = fprintf(store->log_fp, "%s|%u\n", buf, crc);

    // 5. 强制落地
    fflush(store->log_fp);  // fsync(fileno(store->log_fp)); // 强制操作系统把数据写进物理磁盘

    int fd = fileno(store->log_fp);
    if (fd >= 0)
        fsync(fd);

    // 6. 元数据统计
    store->log_size += bytes;
    store->ops_count += 1;

    return KVSTORE_OK;
}

/**
 * =========================================================
 * 7️⃣ 原子执行路径
 * ========================================================= */

/**
 * 原子写入调度器
 * - 该函数是 KVstore 的写入核心，负责协调磁盘持久化（WAL）,与内存索引（B+ 树）的同步
 *
 * 严格遵循 write-ahead Logging 原则：
 *  1. 准入检查
 *  2. 日志先行
 *  3. 更新索引
 *  4. 异步触发维护
 *
 * - 通过状态机 kvstore_state_allow 进行准入控制
 * - 如果磁盘日志写失败，内存 B+ 树绝对不能更新！
 *   否则内存里有数据但磁盘没有，重启后数据就丢失了（违反了持久性）
 *
设计意图：
    kvstore_put ─┐
                ├─ wal
                ├─ apply
                └─ runtime
    kvstore_del ─┘

    kvstore_replay_* ──→ apply only

 * - 使用全局的 log_lock(大锁)，保证 WAL 写入和 B+ 树的修改是一个原子操作
 * - 第 7 步 释放锁，再调用 maybe_compact, 避免所有并发写线程在 pthread_mutex_lock 处排队
 *
 */
static int kvstore_exec_write(kvstore* store, kvstore_op_t op, int key, int value) {
    int ret = KVSTORE_OK;

    if (!store) return KVSTORE_ERR_NULL;

    /* --- 核心临界区开始 --- */
    pthread_mutex_lock(&store->log_lock);

    // [1] 状态准入：仅允许在 READY 状态下写入
    // READONLY 状态意味着 compaction 正在进行原子切换，需拦截写入
    if (store->state != KVSTORE_STATE_READY) {
        pthread_mutex_unlock(&store->log_lock);
        return KVSTORE_ERR_INTERNAL_STATE;
    }

    // [2] 物理写入： WAL 持久化
    // 必须在修改内存索引前完成，确保 crash-safety（冲突安全）
    if (op == KVSTORE_OP_PUT)
        ret = kvstore_log_put(store, key, value);
    else
        ret = kvstore_log_del(store, key);

    // [3] 日志异常处理：若磁盘写入失败，标记系统故障并立即回滚退出
    if (ret != KVSTORE_OK) {
        pthread_mutex_unlock(&store->log_lock);
        return kvstore_fatal(store, ret);
    }

    // [4] 逻辑应用：更新内存 B+ 树索引
    if (op == KVSTORE_OP_PUT) {
        ret = kvstore_apply_put(store, key, (long)value);
    } else {
        ret = kvstore_apply_del(store, key);
    }

    // [5] 统计更新：仅在操作完全成功后累加计数器
    if (ret == KVSTORE_OK) {
        store->ops_count++;
    }

    pthread_mutex_unlock(&store->log_lock);
    /* --- 临界区结束 --- */

    // [6] 后勤维护：尝试触发日志压缩（异步思维）
    // 此时已释放锁， maybe_compact 内部使用 trylock 避免阻塞当前线程
    kvstore_maybe_compact(store);

    return ret;
}

/**
 * kvstore_replay_put - 将 WAL 日志中的历史 PUT 记录重应用到内存索引
 *
 * @details
 *  - 该函数是“影子写入”。它绕过了日志持久化逻辑，仅对 B+ 树执行 apply 操作
 *  - 通过状态机确保该函数只能在恢复阶段使用、
 *  - 绝不调用写日志逻辑，防止产生死循环（重放日志产生日志）
 */
int kvstore_replay_put(kvstore* store, int key, long value) {
    // 1. 基础合法性检查
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 状态机“重放权限”检查
    if (!kvstore_state_allow(store->state, KVSTORE_OP_REPLAY))
        return KVSTORE_ERR_INTERNAL_STATE;

    // 3. 重放阶段的核心逻辑：直接写入内存
    int ret = kvstore_apply_put(store, key, value);

    // 4. 恢复期间的错误诊断
    if (ret != KVSTORE_OK) {
        printf("Replay PUT failed: key=%d, err=%d", key, ret);
        return ret;
    }

    return KVSTORE_OK;
}

/**
 * kvstore_replay_del - 将 WAL 日志中的历史 DEL 记录到重应用到内存索引
 *
 * 该函数确保了“删除”这一事实在系统重启后依然生效。
 */
int kvstore_replay_del(kvstore* store, int key) {
    // 1. 基础合法性检查
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 状态机校验
    if (!kvstore_state_allow(store->state, KVSTORE_OP_REPLAY))
        return KVSTORE_ERR_INTERNAL_STATE;

    // 3. 逻辑执行：内存抹除
    return kvstore_apply_del(store, key);
}

/**
 * 系统的中央决策矩阵，判定当前状态与从左的兼容性
 *
 * - 将全系统的逻辑收拢于此，避免逻辑碎片化。
 * - 严格隔离“恢复态”与“就绪态”，确保 WAL 重放过程不受业务干扰
 *
 * 状态机与业务代码解耦
 *  - kvstore_put 中不再写 if(store->recovering)
 *  - 业务函数只需调用一次 state_allow, 以后想再增加一个状态，
 *    只需要修改这个矩阵函数即可
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
    // 设计哲学：这是一个“白名单”机制。默认不准做任何事，除非明确允许
    switch (op) {
        /* 1. 写操作：最严格的区域 */
        // PUT 和 DEL 只有在 READY 状态下进行
        case KVSTORE_OP_PUT:
        case KVSTORE_OP_DEL:
            return state == KVSTORE_STATE_READY;

        /* 2. 读操作：相对宽松 */
        // 允许系统在不接受新写入的情况下（比如维护期），依然可以提供查询服务
        case KVSTORE_OP_GET:
            return state == KVSTORE_STATE_READY || state == KVSTORE_STATE_READONLY;

        /* 3.系统内部操作：特权指令 */
        // REPLAY 只能在 RECOVERING 状态执行
        case KVSTORE_OP_REPLAY:
            return state == KVSTORE_STATE_RECOVERING;

        /* 4. 生命周期操作：收尾保障 */
        // 只要不是正在关闭（CLOSING），都可以申请关闭或销毁
        case KVSTORE_OP_CLOSE:
        case KVSTORE_OP_DESTROY:
            return state != KVSTORE_STATE_CLOSING;

        default:
            return 0;  // 兜底：未知操作一律拦截
    }
}

/**
 * =========================================================
 * 8️⃣ 内存变更 Apply 层
 * ========================================================= */
/**
 * 内存索引应用层
 *  - 该函数负责将已经过验证的数据变更应用到内存索引结构中。
 *    他起到了解耦层的作用，将上层的事务/日志逻辑与底层的 B+ 树数据结构分离
 * 
 * 准入特性：
 * 1. 幂等性：被调用时加锁数据已持久化，仅负责内存状态更新
 * 2. 线程安全：本函数不加锁，调用者（如 exec_write）必须持有 store->log_lock
 * 3. 复用性：同时支持正常写路径与崩溃恢复（WAL Replay）路径
 */
static int kvstore_apply_put(kvstore* store, int key, long value) {
    // [1] 安全检查
    if (!store) return KVSTORE_ERR_NULL;

    
    if (!store->tree) {
        // 索引结构未初始化或处于非法状态，通常是致命错误
        fprintf(stderr, "[ERROR] Memory index (B+ Tree) is not initialized.\n");
        return KVSTORE_ERR_INTERNAL_STATE;
    }
    // [2] 跨层调用:将请求路由至底层 B+ 树算法
    /**
     * 这里体现了“接口隔离”原则。kvstore 不直接操作 B+ 树节点，
     * 而是通过 apply_put_internal 这一受限接口进行数据注入
     */
    return kvstore_apply_put_internal(store->tree, key, value);
}

/**
 * kvstore_apply_del - 执行内存索引的删除操作
 *
 * @details
 *  - 带 apply_ 前缀的函数通常意味着：“这只是内存操作”。不包含写日志、不包含状体检查
 *  - 调用这个函数是不会产生磁盘 IO 的
 */
static int kvstore_apply_del(kvstore* store, int key) {
    // 1. 安全性检查
    if (!store || !store->tree) return KVSTORE_ERR_NULL;

    // 2. 委派执行 - 原子性与封装
    return kvstore_apply_del_internal(store->tree, key);
}

/**
 * kvstore_apply_put_internal
 *  - 真正的 B+ 树写入操作与语义转换
 *
 * @details
 * - 该函数是 KV 引擎与 B+ 树算法库的“粘合剂”，负责返回底层算法返回细粒度状态，
 *   并将其归一化为存储引擎的标准返回码（即将 KV 错误码体系与底层 B+ 树的错误码体系
 *   进行解耦与转换， 适配器模式 [Adapter Pattern] 的微观实现）
 * - 保护了底层树的结构完整性，所有对树的修改必须通过此翻译层
 */
static int kvstore_apply_put_internal(bptree* tree, int key, long value) {
    // 1. 最后的屏障
    if (!tree) return KVSTORE_ERR_NULL;

    // 2. 调用底层算法引擎 - 纯粹是数据结构操作
    int ret = bptree_insert(tree, key, value);

    // 3. 作物吗映射（Error Mapping）
    switch (ret) {
        case BPTREE_OK:
        case BPTREE_UPDATED:
            // 无论是新插入，还是更新旧值，对 KV 业务来说都是“写入成功”
            return KVSTORE_OK;

        case BPTREE_ERR:
            return KVSTORE_ERR_INTERNAL;

        default:
            // 兜底：底层处理器可能出现的未预期状态
            return KVSTORE_ERR_INTERNAL;
    }
}

/**
 * kvstore_apply_del_internal
 *  - 执行 B+ 树物理删除操作
 */
static int kvstore_apply_del_internal(bptree* tree, int key) {
    // 1. 最后的防线
    if (!tree) return KVSTORE_ERR_NULL;

    // 2. 直接委派
    return bptree_delete(tree, key);
}

/**
 * =========================================================
 * 9️⃣ Snapshot / Compaction
 * ========================================================= */

/**
 * kvstore_maybe_compact - 监控系统负载并自动触发日志压缩
 *
 * 采取基于容量（Size）和频率（Ops）的双重触发机制策略。
 *
 * 设计说明：
 *  - compaction 属于维护行为，不影响 PUT / DEL 正确性
 *  - 即使 compaction 失败，WAL 仍然可保证数据安全
 */
static void kvstore_maybe_compact(kvstore* store) {
    if (!store) return;

    // 1. 预检查：未达阈值则直接返回
    if (store->ops_count < KVSTORE_MAX_OPS) {
        return;
    }

    // 2. 尝试拿锁：如果拿不到，说明有其他线程在写或者在压缩，直接放弃本次触发
    if (pthread_mutex_trylock(&store->log_lock) != 0) {
        return;
    }

    // 3. 拿到锁后再次检查状态，防止重复进入压缩
    if (store->state == KVSTORE_STATE_READONLY) {
        pthread_mutex_unlock(&store->log_lock);
        return;
    }

    // 4. 标记为只读状态（代表正在压缩），然后【立即解锁】
    // 这样在耗时的 scan 期间，写线程可以继续进入 exec_write (但会被状态拦截)
    // 或者你可以选择在这里不解锁，但为了性能，我们推荐在 scan 时允许读取
    store->state = KVSTORE_STATE_READONLY;
    pthread_mutex_unlock(&store->log_lock);

    // 5. 调用实际的压缩逻辑
    kvstore_compact_internal(store);

    // 6. 恢复状态
    pthread_mutex_lock(&store->log_lock);
    store->state = KVSTORE_STATE_READY;
    pthread_mutex_unlock(&store->log_lock);
}

/**
 * kvstore_compact_internal - 执行物理层面的日志压缩与重组
 *
 * 核心思想：
 *  - 将当前内存中的完整状态（B+ 树） 重写为一个新的 WAL
 *  - 丢弃历史冗余日志，降低 replay 成本
 *
 * 崩溃安全性保证（crash-safe）: → crash consistency（崩溃一致性）
 *  - 使用临时文件 + rename 的原子替换语义
 *  - 任意时刻宕机，磁盘上要么是旧 WAL,要么是新的 WAL
 */
static int kvstore_compact_internal(kvstore* store) {
    if (!store) return KVSTORE_ERR_NULL;

    const char* tmp_path = "data.compact";
    const char* data_path = store->log_path;

    // 【耗时操作：锁外执行】 遍历 B+ 树生成新日志
    FILE* fp = fopen(tmp_path, "w");
    if (!fp) return KVSTORE_ERR_IO;

    fprintf(fp, "%s\n", KVSTORE_LOG_VERSION);
    if (bptree_scan(store->tree, compact_write_cb, fp) != 0) {
        fclose(fp);
        unlink(tmp_path);
        return KVSTORE_ERR_INTERNAL;
    }

    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    /* ==========================================================
     * 【核心原子区】 仅保护文件流切换和计数器重置
     * ========================================================== */
    pthread_mutex_lock(&store->log_lock);

    if (store->log_fp) {
        fclose(store->log_fp);
        store->log_fp = NULL;
    }

    if (rename(tmp_path, data_path) == 0) {
        store->log_fp = fopen(data_path, "a+");
        // 关键：重置计数器，确保解锁后 maybe_compact 不会再次触发
        store->log_size = 0;
        store->ops_count = 0;
    } else {
        perror("Compaction rename failed");
        store->log_fp = fopen(data_path, "a+");
    }

    pthread_mutex_unlock(&store->log_lock);
    /* ========================================================== */

    kvstore_create_snapshot(store);
    return KVSTORE_OK;
}

/**
 * compact_write_cb - B+ 树扫描回调函数，负责将村花记录写入压缩日志
 *
 * @details
 *  - 该函数实现了“幂等化归档”。不记录历史过程（A->B->C）,只记录最终结果（C）
 *
 */
static int compact_write_cb(int key, long value, void* arg) {
    // 1. 类型安全还原
    FILE* fp = (FILE*)arg;

    // 2. 序列化（Serialization）
    char payload[128];
    snprintf(payload, sizeof(payload), "PUT %d %ld", key, value);

    // 3. 完整性重塑 - 每一行新日志都必须重新计算 CRC
    uint32_t crc_val = crc32(payload);

    // 4. 标准化格式输出 - 严格遵循格式: Payload | CRC\n
    if (fprintf(fp, "%s|%u\n", payload, crc_val) < 0) {
        return KVSTORE_ERR_INTERNAL;
    }

    return KVSTORE_OK;
}

/**
 * snapshot_write_cb - B+ 树扫描回调函数，专门用于生成持久化快照
 *
 * @details
 *  - 采用与 WAL 相同的文本格式，降低系统复杂性，
 *    使得快照可以被是为一个“预压缩的日志文件”。
 *  - snapshot 仅用于冷启动加速，不参与数据正确性保证
 *
 * @note
 *  compact_write_cb 的目的是更新 WAL,未来可以在 WAL 中记录更复杂的元数据（时间戳）
 *  snapshot_write_cb 的目的是数据备份，未来可以把它改成二进制格式以节省空间
 *
 *
 */
static int snapshot_write_cb(int key, long value, void* arg) {
    // 1. 类型转换：从通用上下文还原文件句柄
    FILE* fp = (FILE*)arg;

    // 2. 构造操作语义（Serialization）
    char payload[128];
    snprintf(payload, sizeof(payload), "PUT %d %ld", key, value);

    // 3. 计算 CRC 校验码
    uint32_t crc_val = crc32(payload);

    // 4. 物理写入
    fprintf(fp, "%s|%u\n", payload, crc_val);

    return KVSTORE_OK;
}

/**
 * =========================================================
 * 🔟 状态机
 * ========================================================= */
/**
 * kvstore_transit_state - 执行安全的状态切换
 *
 * @details
 *  - 确保系统生命周期符合预设路径，防止逻辑越权
 */
static int kvstore_transit_state(kvstore* store, kvstore_state_t next) {
    if (!store)
        return KVSTORE_ERR_NULL;

    kvstore_state_t prev = store->state;

    /* 1. 状态转移合法性检查 */
    switch (prev) {
        case KVSTORE_STATE_INIT:
            // INIT 只能去干活先恢复（RECOVERING）
            if (next != KVSTORE_STATE_RECOVERING)
                return KVSTORE_ERR_INTERNAL_STATE;
            break;

        case KVSTORE_STATE_RECOVERING:
            // 恢复成功去 READY,恢复失败（CRC 错误）去 CORRUPTED
            if (next != KVSTORE_STATE_READY && next != KVSTORE_STATE_CORRUPTED)
                return KVSTORE_ERR_INTERNAL_STATE;
            break;

        case KVSTORE_STATE_READY:
            // 运行中允许自我保持，或者遇到致命错误转为 CORRUPTED
            if (next != KVSTORE_STATE_READY && next != KVSTORE_STATE_CORRUPTED)
                return KVSTORE_ERR_INTERNAL_STATE;
            break;

        case KVSTORE_STATE_CLOSING:
            // 已经要关门了，不能再去任何其他状态（不允许有 next）
            return KVSTORE_ERR_INTERNAL_STATE;

        case KVSTORE_STATE_CORRUPTED:
            // 坏掉的系统是“死胡同”，除非销毁重启，否则不能逃逸
            return KVSTORE_ERR_INTERNAL_STATE;

        default:
            return KVSTORE_ERR_INTERNAL_STATE;
    }

    /* 2. 真正的修改状态 */
    // 只有通过了上面的审查，才允许修改内存中的状态值
    store->state = next;

    /* 3. 派生状态同步 */
    kvstore_apply_state(store);

    return KVSTORE_OK;
}

/**
 * 状态转换的语义化 API
 *  - 进入恢复期
 */
static int kvstore_enter_recovering(kvstore* store) {
    return kvstore_transit_state(store, KVSTORE_STATE_RECOVERING);
}

/**
 * 系统完成恢复，可以对外提供完整服务
 */
static int kvstore_enter_ready(kvstore* store) {
    return kvstore_transit_state(store, KVSTORE_STATE_READY);
}

/**
 * 系统进入不可恢复错误态，只允许 destroy
 */
static int kvstore_enter_failed(kvstore* store) {
    return kvstore_transit_state(store, KVSTORE_STATE_CORRUPTED);
}

/**
 * kvstore_enter_compaction - 进入压缩前置保护状态（只读）
 *
 * 设计意图：
 *  - compaction 是一个耗时的“重量级操作”。在执行期间，必须通过将 state 切换为
 *    KVSTORE_STATE_READONLY(只读) 来拦截所有并发的写请求（PUT / DEL）
 *
 *  - 只有处于 READY(正常服务)的系统才能发起压缩。此函数确保压缩操作不会在
 *    恢复中（RECOVERY）或已损坏（CURRUPTED）的情况下被非法触发
 */
static int kvstore_enter_compaction(kvstore* store) {
    // 1. 基础安全校验
    if (!store) return KVSTORE_ERR_NULL;

    // 2. 状态准入判定
    if (store->state != KVSTORE_STATE_READY)
        return KVSTORE_ERR_READONLY;

    // 3. 状态降级 - 降级为 READONLY
    // 所有的 PUT 和 DEL 请求都会被 kvstore_state_allow 拦截
    // 保证在扫描 B+ 树生成快照时，内存中的树结果时静止的，不会被并发修改
    store->state = KVSTORE_STATE_READONLY;

    // 4. 应用生效
    kvstore_apply_state(store);

    return KVSTORE_OK;
}

/**
 * kvstore_exit_compaction - 退出压缩阶段：恢复系统压缩前的运行状态
 *
 * 设计说明：
 *  - 压缩完成后，通过传入 prev 参数，可以将系统恢复到他所前的状态
 *  - 无论压缩成功还是失败，都必须调用此函数来解锁系统权限，否则系统将永远停留在只读模式
 *
 *  这里通常是写锁的地方 ！
 */
static void kvstore_exit_compaction(kvstore* store, kvstore_state_t prev) {
    // 1. 如果当前状态已经被标记为 CORRUPTED (由 fatal 触发)，则绝对不能回溯
    if (store->state == KVSTORE_STATE_CORRUPTED) {
        return;
    }

    // 2. 正常情况下，如果压缩顺利完成，回到 READY
    // 注意：不要盲目使用 prev，直接回归 Ready 状态是最稳妥的
    store->state = KVSTORE_STATE_READY;

    // 3. 重新应用权限
    kvstore_apply_state(store);
}

/**
 * kvstore_apply_state
 *  - 派生状态同步器。根据主状态配置系统的物理运行参数
 *
 * @details
 *  - 将逻辑状态（READY/FAILED）与物理行为（READYONLY/MODE）解耦
 *  - 除了特定的 READY 状态外，其余所有路径默认开启 readonly=1，实现最小权限原则
 *
 * -该函数是状态机的最终落地。每当 state 发生跳变，必须调用此函数以确保系统北村标志与预期行为同步。
 *
 */
static void kvstore_apply_state(kvstore* store) {
    switch (store->state) {
        case KVSTORE_STATE_INIT:
            // 初始态：正常模式但禁止写入
            store->mode = KVSTORE_MODE_NORMAL;
            store->readonly = 1;
            break;

        case KVSTORE_STATE_RECOVERING:
            // 恢复态必须强制设置为 REPLAY 模式
            // 确保系统处于“只回放、不产生新日志”
            store->mode = KVSTORE_MODE_REPLAY;
            store->readonly = 1;
            break;

        case KVSTORE_STATE_READY:
            // 唯一的“通车”时刻，允许用户写入数据
            store->mode = KVSTORE_MODE_NORMAL;
            store->readonly = 0;
            break;

        case KVSTORE_STATE_READONLY:
            // 维护/只读态：维持 NORMAL 逻辑但切断写入路径
            store->mode = KVSTORE_MODE_NORMAL;
            store->readonly = 1;
            break;

        case KVSTORE_STATE_CLOSING:
        case KVSTORE_STATE_CORRUPTED:
            // 安全降级：无论是正常关闭还是故障
            // 第一步永远是撤销写入权限（readonly = 1）,防止脏数据写入磁盘
            store->readonly = 1;
            break;

        default:
            // 兜底：未知即风险。进入最严格的只读模式保护现场
            store->readonly = 1;
            break;
    }
}

/**
 * kvstore_fatalb
 *  - 这个错误认定为不可恢复
 */
static int kvstore_fatal(kvstore* store, int err) {
    // 1. 鲁棒性检查
    if (!store)
        return err;

    // 2. 状态熔断（Failsafe）
    // 一旦调用此函数，状态机会立即切换到 CORRUPTED (损坏)状态
    kvstore_enter_failed(store);

    // 3. 错误透传 - 将引起致命故障的原始错误码原样返回
    return err;
}

kvstore_state_t kvstore_get_state(kvstore* store) {
    if (!store) return KVSTORE_STATE_CLOSED;
    pthread_mutex_lock(&store->log_lock);  // 复用日志锁来保证状态可见性
    kvstore_state_t s = store->state;
    pthread_mutex_unlock(&store->log_lock);
    return s;
}