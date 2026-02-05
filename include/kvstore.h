// /home/ubuntu/c++2512/KVstore/include/kvstore.h

/**
 * kvstore 结构体负责整个存储引擎，包含一个指向 bptree 的指针
 * B+ 树则是用来存储键值对的数据结构
 */

#ifndef KVSTORE_H
#define KVSTORE_H

#include <stdint.h>
#include <stdio.h>

#include "index/bptree.h"



#define KVSTORE_MAX_OPS 1000
#define KVSTORE_MAX_LOG_SIZE 4 * 1024 * 1024  // 4MB

// kvstore error codes
#define KVSTORE_OK 0

// =============================
// 通用错误（-1 ~ -9）
// =============================
#define KVSTORE_ERR_NULL        -1      // 空指针 / 非法参数
#define KVSTORE_ERR_INTERNAL    -2      // 内部逻辑错误（不应发生）
#define KVSTORE_ERR_IO          -3      // 文件 / IO 错误

// =============================
// 状态错误（-10 ~ -19）
// =============================
#define KVSTORE_ERR_READONLY        -10  // 只读模式，禁止写
#define KVSTORE_ERR_RECOVERY        -11  // 恢复中（预留）
#define KVSTORE_ERR_INTERNAL_STATE  -12  // 状态机非法

// =============================
// 数据错误（-20 ~ -29）
// =============================
#define KVSTORE_ERR_NOT_FOUND           -20  // key 不存在
#define KVSTORE_ERR_WAL_CORRUPTED       -21  
#define KVSTORE_ERR_SNAPSHOT_CORRUPTED  -22

// =============================
// 资源错误（-30 ~ -39）
// =============================
// 未来扩展
// #define KVSTORE_ERR_NO_MENORY  -30
// #define KVSTORE_ERR_NO_SPACE   -31

// #define KVSTORE_ERR_STATE -41

#define KVSTORE_LOG_VERSION "KVSTORE_LOG_V1"  // 日志格式版本（当前是文本格式），以后使用二进制格式可用 V2

#ifdef __cplusplus
extern "C" {
#endif

// 前向声明（隐藏内部结构）- opaque pointer(不透明指针)，写库的标准做法
/**
 * struct kvstore：前向声明，告诉编译器，有一个 struct kvstore 的东西，但是它长什么样子（里面有哪些成员），
 *                  现在不需要知道，只要知道这是一个合法的结构体类型就行了
 * typedef ... kvstore: 取别名，如果不写这行，以后每次定义变量都要写 struct kvstore* s,
 *                              写了这一行，只需要写 kvstore* s
 * 结构体定义在 .c 内部的好处：
 *      1. 绝对的封装（数据包隐藏），如果把结构体写在 .h 里，任何用户都可以写出 store->log_fp = NULL -> 关掉了我的日志文件
 *          放到 .c 内部时，用户在他们的代码里只能拿到一个 kvstore* 指针，看不到结构体内部细节，无法直接访问任何成员，只能通过我提供的API来操作
 *      2. 极速编译（减少头文件污染），放在 .h 里，且里面包含了一些特殊的头文件，任何 include "kvstore.h" 的文件都会被迫引入这些额外的头文件，
 *          一旦修改了结构体里的一个微小变量，所有引用了该头文件的代码都必须重新编译；而放在 .c 不需要重新编译，编译速度起飞
 *      3. 二进制兼容性，以后 V4 版本，结构体里面加成员，老程序必须重新编译；定义在 .c ,用户手里始终只是一个8字节的指针地址，
 *          结构体变大变小都在内部处理，老程序甚至不需要重新编译就能直接用我的新库
 */
typedef struct _kvstore kvstore;

/**
 * 系统内部语义
 * enum 枚举类型,里面包含三个枚举成员
 */
typedef enum {
    KVSTORE_MODE_NORMAL = 0,    // 正常模式
    KVSTORE_MODE_REPLAY,        // 重放模式
    KVSTORE_MODE_SNAPSHOT_LOAD  // 快照加载模式（自动等于 2?）
} kvstore_mode_t;

typedef enum {
    KVSTORE_OP_PUT = 1,
    KVSTORE_OP_DEL = 2,
    KVSTORE_OP_REPLAY = 3,
    KVSTORE_OP_GET = 4,
    KVSTORE_OP_CLOSE = 5,
    KVSTORE_OP_DESTROY = 6,
} kvstore_op_t;

/**
 * kvstore_state invariant: (不变式)
 *
 * INIT         ->  no public API allowed
 * RECOVERING   ->  no public API allowed
 * READY        ->  GET / PUT /DEL allowed
 * READONLY     ->  GET allowed, PUT / DEL forbidden
 * CLOSING      ->  no public API allowed
 * CORRUPTED    ->  no public API allowed
 *
 * 1. Public API 只允许在 KVSTORE_STATE_READY 下执行
 * 2. mode / readonly 只能由 kvstore_apply_state 推到
 * 3. 任一 fatal 错误必须进入 KVSTORE_STATE_CORRUPTED
 *
 * state 是系统级语义，node / readonly 只能从 state 推导，不能反向决定 state
 *
 *
 */
typedef enum {
    KVSTORE_STATE_INIT,        // 尚未开始恢复
    KVSTORE_STATE_RECOVERING,  // snapshot load / WAL replay
    KVSTORE_STATE_READY,       // 正常对外服务
    KVSTORE_STATE_READONLY,    // compaction / 维护

    KVSTORE_STATE_CLOSING,  // 关闭中
    KVSTORE_STATE_CLOSED,
    KVSTORE_STATE_CORRUPTED,  // 不可恢复错误
} kvstore_state_t;

// =============  kvstore 基本操作函数  =============

/* ========== 生命周期 ========== */
kvstore* kvstore_create();
void kvstore_destroy(kvstore* store);

kvstore* kvstore_open(const char* wal_path);
int kvstore_recover(kvstore* store);
int kvstore_log_open(kvstore* store, const char* wal_path);
int kvstore_log_close(kvstore* store);
int kvstore_close(kvstore* store);

/* ========== 基本操作 ========== */
int kvstore_put(kvstore* store, int key, long value);
int kvstore_del(kvstore* store, int key);
int kvstore_create_snapshot(kvstore* store);

/* ========== 高级接口 ========== */
int kvstore_insert(kvstore* store, int key, long value);
int kvstore_search(kvstore* store, int key, long* value);
int kvstore_compact(kvstore* store);

/* ========== 错误处理 ========== */
const char* kvstore_strerror(int err);

/* ========== debug 调试 ==========*/
void kvstore_debug_set_mode(kvstore* store, kvstore_mode_t mode);

// ==========  通用工具 =========
uint32_t crc32(const char* s);

#ifdef __cplusplus
}
#endif

#endif  // KVSTORE_H