
/// home/ubuntu/c++2512/KVstore/test/test_kvstore.c
#include <assert.h>
#include <stdint.h>  // uint32_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>  // unlink

#include <pthread.h>

#include "kvstore.h"

#define TEST_LOG "test.log"
#define SNAPSHOT_FILE "data.snapshot"

#define THREAD_OPS 20000

typedef struct {
    kvstore* store;
    int id;
} worker_arg;

/**
 * 每个测试开始之前，把上一次留下的WAL删掉，确保这是一个干净的 kvstore
 *
 * 测试环境隔离器
 *  - 删除旧 WAL
 *  - 删除旧 snapshot
 *  - 每个测试从空库开始
 */
static void
cleanup() {
    remove(TEST_LOG);  // 或者使用 unlink()
    remove(SNAPSHOT_FILE);
}

/* ====== Test 1. basic put/search ========*/
/**
 * 测试插入 / 覆盖写
 */
void test_basic_put_search() {
    cleanup();  //

    kvstore* s = kvstore_open(TEST_LOG);
    assert(s != NULL);

    int ret = kvstore_put(s, 1, 100);
    if (ret != 0) {
        printf("Debug: kvstore_put 失败了！错误码是: %d\n", ret);
    }
    assert(ret == 0);

    int rett = kvstore_put(s, 1, 200);
    if (rett != 0) {
        printf("Debug: kvstore_put 失败了！错误码是: %d\n", rett);
    }
    assert(rett == 0);

    long v = 0;
    assert(kvstore_search(s, 1, &v) == 0);
    assert(v == 200);

    kvstore_destroy(s);

    printf("\n[PASS 1] 基本查找测试成功！\n");
}

// ===== test 2. 覆盖写重放 =====
/**
 * 支持崩溃恢复
 */
void test_update_replay() {
    cleanup();

    {
        kvstore* s = kvstore_open(TEST_LOG);
        assert(s != NULL);

        kvstore_put(s, 1, 100);
        kvstore_put(s, 1, 200);  // 覆盖写
        kvstore_destroy(s);
    }

    {
        kvstore* s = kvstore_open(TEST_LOG);
        if (s == NULL) {
            fprintf(stderr, "FATAL: kvstore create 失败，日志可能损坏或格式不对！\n");
            exit(1);
        }
        long v;
        assert(kvstore_search(s, 1, &v) == 0 && v == 200);
        kvstore_destroy(s);
    }
    printf("\n[PASS 2] 覆盖写重放成功！\n");
}

// ======= 3. 删除重放 ========
/**
 * 支持逻辑删除恢复
 */
void test_delete_replay() {
    cleanup();

    // 1. 写入数据后删除，然后关机（Destroy）
    {
        kvstore* s = kvstore_open(TEST_LOG);
        kvstore_put(s, 1, 100);
        kvstore_put(s, 2, 200);

        // 执行删除
        int ret = kvstore_del(s, 1);
        assert(ret == 0);

        kvstore_destroy(s);
    }

    // 2. 重启，验证 key 1 消失，key 2 还存在
    {
        kvstore* s = kvstore_open(TEST_LOG);
        long v;

        // key 1 应该找不到了
        assert(kvstore_search(s, 1, &v) != 0);

        // key 2 应该还存在
        assert(kvstore_search(s, 2, &v) == 0 && v == 200);

        kvstore_destroy(s);
    }

    printf("\n[PASS 3] 删除重放测试成功！\n");
}

/* ========  Test 4: replay  ======== */
/**
 * 支持重放
 */
void test_replay() {
    cleanup();

    {
        kvstore* s = kvstore_open(TEST_LOG);
        assert(s != NULL);
        kvstore_put(s, 1, 100);
        kvstore_put(s, 2, 200);
        kvstore_destroy(s);
    }

    {
        kvstore* s = kvstore_open(TEST_LOG);
        assert(s != NULL);
        long v;
        assert(kvstore_search(s, 1, &v) == 0 && v == 100);
        assert(kvstore_search(s, 2, &v) == 0 && v == 200);
        kvstore_destroy(s);  //
    }

    printf("\n[PASS 4] 重放测试成功！\n");
}

// ======== test 5. crc 损坏测试 =========
/**
 * 防止静默数据损坏
 */
void test_corruption() {
    cleanup();
    // 1. 先制造一点正常数据
    {
        kvstore* s = kvstore_open(TEST_LOG);
        kvstore_put(s, 1, 100);
        kvstore_destroy(s);
    }

    // 2. 暴力破坏：手动改写日志文件
    FILE* fp = fopen(TEST_LOG, "r+");
    fseek(fp, -5, SEEK_END);  // 跳到倒数第5个字节
    fprintf(fp, "X");         // 强行破坏 CRC 字符
    fclose(fp);

    // 3. 尝试加载，预期应该报错
    kvstore* s = kvstore_open(TEST_LOG);
    if (s == NULL) {
        printf("[PASS] test_corruption: System correctly refused corrupted log.\n");
    } else {
        // 如果加载成功了，说明你的 CRC 没起作用
        printf("[FAIL] test_corruption: System accepted corrupted log!\n");
        kvstore_destroy(s);
    }
    printf("\n[PASS 5] crc 损坏测试成功！\n");
}

// 获取文件大小
long get_file_size(const char* filename) {
    FILE* fp = fopen(filename, "rb");
    if (!fp) return 0;

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fclose(fp);

    return size;
}

// ========  test 6. 日志压缩测试  ========
/**
 * WAL 压缩（垃圾回收）- 长期运行不会爆磁盘
 */
void test_log_compaction() {
    cleanup();
    const char* log_path = TEST_LOG;

    // 1. 制造大量冗余数据
    {
        kvstore* s = kvstore_open(TEST_LOG);
        for (int i = 0; i < 100; i++) {
            kvstore_put(s, 1, i);  // 重复写 key 1，从 0 到 99
        }
        kvstore_destroy(s);
    }

    // 记录压缩前的文件大小
    long size_before = get_file_size(log_path);

    // 2. 触发压缩
    {
        kvstore* s = kvstore_open(TEST_LOG);
        printf("[DEBUG] 正在执行日志压缩...\n");
        int ret = kvstore_compact(s);
        assert(ret == KVSTORE_OK);
        kvstore_destroy(s);
    }

    // 3. 验证文件变小了
    long size_after = get_file_size(log_path);
    printf("[DEBUG] 压缩前大小：%ld, 压缩后大小：%ld\n", size_before, size_after);
    assert(size_after < size_before);

    // 4. 最后验证：重启看数据对不对
    {
        kvstore* s = kvstore_open(TEST_LOG);
        long v;
        assert(kvstore_search(s, 1, &v) == KVSTORE_OK && v == 99);
        kvstore_destroy(s);
    }

    printf("\n[PASS 6] 日志压缩测试成功！\n");
}

// ========  test 7. 异常关闭测试  ========
/**
 * 部分写入日志恢复能力
 *  - 半条日志绝对不能恢复
 */
void test_crash_recovery_1() {
    cleanup();
    const char* log_path = "test.log";

    // 1. 构造一个“坏掉”的日志：最后一行数据被强行截断
    FILE* fp = fopen(log_path, "w");
    fprintf(fp, "KVSTORE_LOG_V1\n");

    // 1. 动态生成第一行的CRC
    char buf1[64];
    sprintf(buf1, "PUT 1 100");
    uint32_t crc1 = crc32(buf1);
    fprintf(fp, "%s|%u\n", buf1, crc1);  // 正常行

    // 2. 构造第二行
    fprintf(fp, "PUT 2 200");  // 不写 | 和 CRC，模拟写到一半断电
    fclose(fp);

    // 2. 尝试启动
    printf("[DEBUG] 尝试从损坏的日志恢复...\n");
    kvstore* s = kvstore_open(TEST_LOG);

    // 3. 验证逻辑
    assert(s != NULL);
    long v;
    assert(kvstore_search(s, 1, &v) == 0 && v == 100);  // key 1 必须存在
    assert(kvstore_search(s, 2, &v) != 0);              // key 2 必须不存在

    kvstore_destroy(s);
    printf("\n[PASS 7] 异常关闭模拟测试成功!\n");
}

// ========  test 8. 空日志 replay ======
/**
 * 空库启动能力 - 部署环境必测
 */
void test_empty_replay() {
    // 删除 test.log (WAL) 和 data.snapshot(快照)
    cleanup();

    // 强制确认文件不存在
    FILE* f = fopen(TEST_LOG, "r");
    if (f) {
        printf("[WARNING] 发现残留日志文件! Replay 将会加载旧数据。\n");
        fclose(f);
    }

    kvstore* s = kvstore_open(TEST_LOG);
    assert(s);

    long v = -1;
    assert(kvstore_search(s, 1, &v) != 0);
    assert(v == -1);

    kvstore_destroy(s);
    printf("\n[PASS 8] 空日志测试成功\n");
}

// =======   test 9. =======
/**
 * 未正常关闭恢复能力
 */
void test_crash_recovery() {
    cleanup();

    // 1. 第一轮：模拟写入后直接“断电” （不调用 close 模拟异常退出）
    kvstore* s1 = kvstore_open(TEST_LOG);
    kvstore_put(s1, 10, 1000);
    // 假设这里没有调用 kvstore_close, 直接退出了
    // 此时数据已经在  WAL 日志里了

    // 2. 第二轮： 重新打开，触发 Repaly 机制
    kvstore* s2 = kvstore_open(TEST_LOG);
    long v;
    // 即使没调用过之前的 close, Replay 应该能从日志恢复数据 【10，1000】
    int ret = kvstore_search(s2, 10, &v);  // 没有 get, 是 search()
    assert(ret == KVSTORE_OK);
    assert(v == 1000);

    kvstore_close(s2);
    printf("\n[PASS 9] 崩溃恢复 Replay 测试成功\n");
}

// ========  test 10. reopen persistence  =========
/**
 * open → put → close → open → search
 *
 * 基础持久化验证
 *  - 写入 -> 关闭 -> 重启 -> 数据还在
 */
void test_reopen_persistence() {
    cleanup();

    kvstore* s1 = kvstore_open(TEST_LOG);
    assert(s1 != NULL);

    assert(kvstore_put(s1, 1, 1000) == KVSTORE_OK);
    assert(kvstore_put(s1, 2, 2000) == KVSTORE_OK);

    kvstore_close(s1);

    // reopen
    kvstore* s2 = kvstore_open(TEST_LOG);
    assert(s2 != NULL);

    long v;
    assert(kvstore_search(s2, 1, &v) == KVSTORE_OK && v == 1000);
    assert(kvstore_search(s2, 2, &v) == KVSTORE_OK && v == 2000);

    kvstore_close(s2);

    printf("\n[PASS 10] reopen persistence test\n");
}

// ========= test 11. open-revocer-idempotent =======
/**
 * open → put → close → open → search
 *
 * 恢复幂等性
 *  - 重放不能产生副作用
 *
 */
void test_open_recover_idempotent() {
    cleanup();

    // 1. open + write
    kvstore* s1 = kvstore_open(TEST_LOG);
    assert(s1 != NULL);

    kvstore_put(s1, 10, 1000);
    kvstore_put(s1, 20, 2000);
    kvstore_close(s1);

    // 2. open (recover)
    kvstore* s2 = kvstore_open(TEST_LOG);
    assert(s2 != NULL);

    long v;
    assert(kvstore_search(s2, 10, &v) == KVSTORE_OK && v == 1000);
    assert(kvstore_search(s2, 20, &v) == KVSTORE_OK && v == 2000);

    // 没有重复
    assert(kvstore_search(s2, 10, &v) == KVSTORE_OK && v == 1000);

    kvstore_close(s2);

    printf("\n[PASS 11] open recover idempotent(开放式重复幂等)\n");
}

// ======== test 12. reject write after close =====
/**
 * 生命周期安全
 *  - 关闭的 store 不允许操作
 *
 */
void test_reject_writer_after_close() {
    cleanup();

    kvstore* s = kvstore_open(TEST_LOG);
    assert(kvstore_put(s, 1, 1000) == KVSTORE_OK);

    kvstore_close(s);

    // 已关闭实例，禁止写
    assert(kvstore_put(s, 2, 2000) != KVSTORE_OK);
    assert(kvstore_del(s, 1) != KVSTORE_OK);

    printf("\n[PASS 12] reject write after close\n");
}

// ======== test 13. 快照恢复测试 ======
/**
 * 分层恢复模型
 */
void test_snapshot_recovery() {
    cleanup();

    const char* snap_path = "data.snapshot";

    // 1. 模拟一个快照文件（key 1 -> 100）
    FILE* fp = fopen(snap_path, "w");
    fprintf(fp, "PUT 1 100\n");
    fclose(fp);

    // 2. 模拟一个 WAL 日志（key 2 -> 200, 覆盖 key 1 -> 150）
    kvstore* s1 = kvstore_open(TEST_LOG);
    assert(s1);
    kvstore_put(s1, 2, 200);
    kvstore_put(s1, 1, 150);
    kvstore_close(s1);

    // 3. 重启系统
    kvstore* s2 = kvstore_open(TEST_LOG);
    assert(s2);

    long v;
    // 验证 key 1 应该是 WAL 里的 150， 而不是 快照里的 100
    assert(kvstore_search(s2, 1, &v) == KVSTORE_OK && v == 150);
    // 验证 key 2 正常从 WAL 恢复
    assert(kvstore_search(s2, 2, &v) == KVSTORE_OK && v == 200);

    kvstore_close(s2);

    printf("\n[PASS 13] 快照与 WAL 联合恢复测试成功!\n");
}

// ========= test 14. 快照后截断日志测试 ========
/**
 * 具备快照独立恢复能力
 */
void test_snapshot_and_log_truncation() {
    cleanup();

    const char* log_path = TEST_LOG;
    const char* snap_path = "data.snapshot";

    // 1. 准备数据并创建快照
    {
        kvstore* s = kvstore_open(log_path);
        assert(s);
        kvstore_put(s, 100, 10000);
        kvstore_put(s, 200, 20000);

        // 触发快照
        int ret = kvstore_create_snapshot(s);
        assert(ret == KVSTORE_OK);

        long v_debug;
        if (kvstore_search(s, 100, &v_debug) == 0) {
            printf("[DEBUG] 快照加载阶段成功，v = %ld\n", v_debug);
        } else {
            printf("[DEBUG] 快照加载阶段就失败了！\n");
        }

        kvstore_destroy(s);
    }

    // 2. 核心动作：模拟日志丢失或被清理
    // 直接删除 WAL 文件， 只留下快照文件

    if (remove(log_path) == 0) {
        printf("[DEBUG] WAL 日志已成功截断（删除）。\n");
    } else {
        perror("remove log\n");
    }

    // 确认快照文件确实存在
    assert(get_file_size(snap_path) > 0);

    // 3. 重启系统：此时系统只能通过快照恢复
    {
        printf("[DEBUG] 尝试通过快照恢复数据...\n");
        kvstore* s = kvstore_open(log_path);
        assert(s != NULL);

        long v1, v2;
        // 验证数据是否还存在
        assert(kvstore_search(s, 100, &v1) == KVSTORE_OK && v1 == 10000);
        assert(kvstore_search(s, 200, &v2) == KVSTORE_OK && v2 == 20000);

        kvstore_destroy(s);
    }

    printf("\n[PASS 14] 快照后截断日志测试成功!\n");
}

// ========== test 15. QPS  =======
/**
 * 测试读写性能
 */
void test_benchmark_write() {
    cleanup();
    const char* log_path = TEST_LOG;

    // 设定测试数量
    const int BENCH_COUNT = 10000;
    printf("\n[BENCHMARK] 开始执行性能测试 (数据量: %d)...\n", BENCH_COUNT);

    // 1. 准备环境
    kvstore* s = kvstore_open(log_path);
    assert(s != NULL);

    // 2. 测试写入性能（Write QPS)

    clock_t start_write = clock();

    for (int i = 0; i < BENCH_COUNT; i++) {
        kvstore_put(s, i, i * 100L);
    }

    clock_t end_write = clock();
    double write_time = (double)(end_write - start_write) / CLOCKS_PER_SEC;
    // 防止除以 0
    if (write_time < 0.000001) write_time = 0.000001;

    printf("  -> [Write] 耗时: %.4f 秒 | QPS: %.2f op/s\n",
           write_time, BENCH_COUNT / write_time);

    // 3. 测试读取性能（Read QPS）
    clock_t start_read = clock();

    long val;
    for (int i = 0; i < BENCH_COUNT; i++) {
        kvstore_search(s, i, &val);
    }

    clock_t end_read = clock();
    double read_time = (double)(end_read - start_read) / CLOCKS_PER_SEC;
    if (read_time < 0.000001) read_time = 0.000001;

    printf("  -> [Read ] 耗时: %.4f 秒 | QPS: %.2f op/s\n",
           read_time, BENCH_COUNT / read_time);

    // 4. 收尾
    kvstore_destroy(s);
    printf("\n[PASS 15] 性能基准测试完成！!\n");
}


// ========== test 16. 并发读写一致性测试   =======
/**
 * - 场景：多线程混合执行 PUT 和 SEARCH 操作
 * - 目的：验证 Root Latch 是否能有效防止 B+ 树在并发修改时发生段错误
 */
void* worker_thread(void* arg)
{
   worker_arg* w = (worker_arg*)arg;
    long val_out;
    unsigned int seed = time(NULL) ^ (w->id * 7919);

    for (int i = 0; i < THREAD_OPS; i++) {
        // 增大 Key 范围，模拟大规模数据和树分裂
        int k = rand_r(&seed) % 10000; 
        int op = rand_r(&seed) % 100;

        if (op < 40) { // 40% 概率写入
            kvstore_put(w->store, k, i);
        } 
        else if (op < 90) { // 50% 概率搜索
            // 搜索时不只是打印日志，可以验证基础逻辑
            kvstore_search(w->store, k, &val_out);
        }
        else { // 10% 概率删除 (如果已经实现了并发删除)
            // kvstore_delete(w->store, k);
        }

        // 每 1000 次操作打印一次进度，避免频繁 IO 导致测试变慢
        if (i % 1000 == 0) {
            printf("Thread %d reached %d ops\n", w->id, i);
        }
    }
    return NULL;
}


/**
 * 并发控制安全：
 *  - 启动多个工作线程，模拟高并发读写压力
 *  - 验证系统在 Root Latch 的保护下是否稳定
 */
void test_concurrency_stress() {
    cleanup();
    printf("\n[RUN 16] concurrency stress testing...\n");

    kvstore* s = kvstore_open(TEST_LOG);
    const int num_threads = 4;
    pthread_t threads[num_threads];
    worker_arg args[num_threads];

    // 1. 创建并发工作线程
    for (int i = 0; i < num_threads; i++) {
        args[i].store = s;
        args[i].id = i;
        pthread_create(&threads[i], NULL, worker_thread, &args[i]);
    }

    // 2. 等待所有线程完成任务
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // 3. 验证最终状态
    assert(kvstore_get_state(s) == KVSTORE_STATE_READY);

    kvstore_close(s);
    printf("[PASS 16] 并发压力（无崩溃且状态有效）测试完成\n");
}


// ========= test 17. 并发写覆盖一致性 =========
/**
 * 场景：多线程反复对同一批 key 执行 PUT
 * 
 * 目的：
 *  - 验证在 Root Latch 保护下不会丢写
 *  - 最终值必须是某个线程写入的有效值
 */
void* worker_write_keys(void* arg) {
    worker_arg* w = (worker_arg*)arg;

    for (int i = 0; i < THREAD_OPS; i++) { // THREAD_OPS = 20000
        int k = i % 10;
        kvstore_put(w->store, k, w->id * 100000 + i);
    }

    return NULL;
}

void test_concurrrent_overwrite() {
    cleanup();
    printf("\n[RUN 17] concurrent overwrite testing...\n");

    kvstore* s = kvstore_open(TEST_LOG);

    const int num_threads = 4;
    pthread_t threads[num_threads];
    worker_arg args[num_threads];

    for (int i = 0; i < num_threads; i++) {
        args[i].store = s;
        args[i].id = i;
        pthread_create(&threads[i], NULL, worker_write_keys, &args[i]);
    }

    // 验证 key 存在且可读
    long val;
    for (int k = 0; k < 10; k++) {
        assert(kvstore_search(s, k, &val) == 0);
    }
    kvstore_close(s);

    printf("[PASS 17] 并发覆盖一致性测试完成\n");
}


// ========== test 18. 并发删除与插入混合测试 ==========
/**
 * 场景： 
 *  - 一半线程不断 PUT
 *  - 一半线程不断 DELETE
 * 
 * 目的：
 *  - 压测 borrow / merge / fixup 逻辑
 *  - 检查是否出现段错误
 */
void* worker_mix(void* arg) {
    worker_arg* w = (worker_arg*)arg;

    for (int i = 0; i < THREAD_OPS; i++) {
        int k = rand() % 50;

        if(w->id % 2)
            kvstore_put(w->store, k, i);
        else
            kvstore_del(w->store, k);
    }

    return NULL;
}

void test_concurrent_insert_delete(){
    cleanup();
    printf("\n[RUN 18] concurrent insert/delete testing...\n");

    kvstore* s = kvstore_open(TEST_LOG);

    const int num_threads = 6;
    pthread_t threads[num_threads];
    worker_arg args[num_threads];

    for (int i = 0; i < num_threads; i++) {
        args[i].store = s;
        args[i].id = i;
        pthread_create(&threads[i], NULL, worker_mix, &args[i]);
    }

    for (int i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);

    assert(kvstore_get_state(s) == KVSTORE_STATE_READY);

    kvstore_close(s);

    printf("[PASS 18] 并发插入删除压力测试完成\n");
}

int main() {
    printf("\n========== KVstore 综合性能测试 ==========\n");

    //======== V1 ~ V3  ========

    test_basic_put_search();  // 1
    test_update_replay();     // 2
    test_delete_replay();     // 3
    test_replay();            // 4

    test_corruption();        // 5
    test_log_compaction();    // 6
    test_crash_recovery_1();  // 7
    test_empty_replay();      // 8

    //  =========  V4  =========
    test_crash_recovery();               // 9
    test_reopen_persistence();           // 10
    test_open_recover_idempotent();      // 11
    test_reject_writer_after_close();    // 12
    test_snapshot_recovery();            // 13
    test_snapshot_and_log_truncation();  // 14

    //test_benchmark_write();        // 15 QPS
    test_concurrency_stress();       // 16 并发控制安全
    //test_concurrrent_overwrite();    // 17
    //test_concurrent_insert_delete(); // 18

    printf("所有测试均通过 ! 🎇\n");
    return 0;
}

/**
 * 手动编译：
 *
 * - 编译源文件：
 *      gcc -c src/index/bptree.c -o src/index/bptree.o -Wall -g -I./include -Iinclude/index
 *      gcc -c src/kvstore.c -o src/kvstore.o -Wall -g -I./include
 * - 编译测试文件：
 *      g++ -c test/test_kvstore.c -o test/test_kvstore.o -Wall -g -I./include -Iinclude/index
 *
 * - 链接所有目标文件：
 *      g++ -o test_kvstore src/index/bptree.o src/kvstore.o test/test_kvstore.o -Wall -g -I./include
 *
 *
 * 运行：
 *      ./test_kvstore
 */