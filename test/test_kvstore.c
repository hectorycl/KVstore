
/// home/ubuntu/c++2512/KVstore/test/test_kvstore.c
#include <assert.h>
#include <stdint.h>  // uint32_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>  // unlink

#include "kvstore.h"

#define TEST_LOG "test.log"

/**
 * 每个测试开始之前，把上一次留下的WAL删掉，确保这是一个干净的 kvstore
 */
static void cleanup() {
    unlink(TEST_LOG);
}

/* ====== Test 1. basic put/search ========*/
void test_basic_put_search() {
    cleanup();  // ??

    kvstore* s = kvstore_create(TEST_LOG);
    assert(s);

    assert(kvstore_put(s, 1, 100) == 0);

    long v = 0;
    assert(kvstore_search(s, 1, &v) == 0);
    assert(v == 100);

    kvstore_destroy(s);

    printf("[PASS 1] 基本查找测试成功！\n");
}

// ===== test 2. 覆盖写重放 =====
void test_update_replay() {
    cleanup();

    {
        kvstore* s = kvstore_create(TEST_LOG);
        kvstore_put(s, 1, 100);
        kvstore_put(s, 1, 200);
        kvstore_destroy(s);
    }

    {
        kvstore* s = kvstore_create(TEST_LOG);
        if (s == NULL) {
            fprintf(stderr, "FATAL: kvstore_create 失败，日志可能损坏或格式不对！\n");
            exit(1);
        }
        long v;
        assert(kvstore_search(s, 1, &v) == 0 && v == 200);
        kvstore_destroy(s);
    }
    printf("[PASS 2] 覆盖写重放成功！\n");
}

// ======= 3. 删除重放 ========
void test_delete_replay() {
    cleanup();

    // 1. 写入数据后删除，然后关机（Destroy）
    {
        kvstore* s = kvstore_create(TEST_LOG);
        kvstore_put(s, 1, 100);
        kvstore_put(s, 2, 200);

        // 执行删除
        int ret = kvstore_del(s, 1);
        assert(ret == 0);

        kvstore_destroy(s);
    }

    // 2. 重启，验证 key 1 消失，key 2 还存在
    {
        kvstore* s = kvstore_create(TEST_LOG);
        long v;

        // key 1 应该找不到了
        assert(kvstore_search(s, 1, &v) != 0);

        // key 2 应该还存在
        assert(kvstore_search(s, 2, &v) == 0 && v == 200);

        kvstore_destroy(s);
    }

    printf("[PASS 3] 删除重放测试成功！\n");
}

// ========  test 4. 只读锁死测试  =========
void test_readonly_lock() {
    cleanup();

    kvstore* s = kvstore_create(TEST_LOG);
    assert(s);

    // 1. 正常模式：可以写进去
    assert(kvstore_put(s, 1, 100) == KVSTORE_OK);

    // 2. 切换到 REPLAY 模式
    kvstore_debug_set_mode(s, KVSTORE_MODE_REPLAY);

    // 3. 尝试写入：预期放回 KVSTORE_ERR_READONLY
    int ret = kvstore_put(s, 2, 200);

    printf("[DEBUG] 只读模式下写入 key 2, 返回码： %d\n", ret);

    // 必须返回 只读错误
    assert(ret == KVSTORE_ERR_READONLY);

    // 4. 验证数据缺失没有被写入
    long v;
    int search_ret = kvstore_search(s, 2, &v);
    assert(search_ret != KVSTORE_OK);  // != OK 继续往下

    // 5. 切回正常模式，验证是否恢复
    kvstore_debug_set_mode(s, KVSTORE_MODE_NORMAL);
    assert(kvstore_put(s, 2, 200) == KVSTORE_OK);
    assert(kvstore_search(s, 2, &v) == KVSTORE_OK && v == 200);

    kvstore_destroy(s);

    printf("[PASS 4] 只读模式锁死测试成功！\n");
}

/* ========  Test 5: replay  ======== */
void test_replay() {
    cleanup();  // ?? 有啥用？

    {
        kvstore* s = kvstore_create(TEST_LOG);
        assert(s != NULL);
        kvstore_put(s, 1, 100);
        kvstore_put(s, 2, 200);
        kvstore_destroy(s);
    }

    {
        kvstore* s = kvstore_create(TEST_LOG);
        assert(s != NULL);
        long v;
        assert(kvstore_search(s, 1, &v) == 0 && v == 100);
        assert(kvstore_search(s, 2, &v) == 0 && v == 200);
        kvstore_destroy(s);  // ??
    }

    printf("[PASS 5] 重放测试成功！\n");
}

// ======== test 6. crc 损坏测试 ========
void test_corruption() {
    cleanup();
    // 1. 先制造一点正常数据
    {
        kvstore* s = kvstore_create(TEST_LOG);
        kvstore_put(s, 1, 100);
        kvstore_destroy(s);
    }

    // 2. 暴力破坏：手动改写日志文件
    FILE* fp = fopen(TEST_LOG, "r+");
    fseek(fp, -5, SEEK_END);  // 跳到倒数第5个字节
    fprintf(fp, "X");         // 强行破坏 CRC 字符
    fclose(fp);

    // 3. 尝试加载，预期应该报错
    kvstore* s = kvstore_create(TEST_LOG);
    if (s == NULL) {
        printf("[PASS] test_corruption: System correctly refused corrupted log.\n");
    } else {
        // 如果加载成功了，说明你的 CRC 没起作用
        printf("[FAIL] test_corruption: System accepted corrupted log!\n");
        kvstore_destroy(s);
    }
    printf("[PASS 6] crc 损坏测试成功！\n");
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

// ========  test 7. 日志压缩测试  ========
void test_log_compaction() {
    cleanup();
    const char* log_path = TEST_LOG;

    // 1. 制造大量冗余数据
    {
        kvstore* s = kvstore_create(log_path);
        for (int i = 0; i < 100; i++) {
            kvstore_put(s, 1, i);  // 重复写 key 1，从 0 到 99
        }
        kvstore_destroy(s);
    }

    // 记录压缩前的文件大小
    long size_before = get_file_size(log_path);

    // 2. 触发压缩
    {
        kvstore* s = kvstore_create(log_path);
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
        kvstore* s = kvstore_create(log_path);
        long v;
        assert(kvstore_search(s, 1, &v) == KVSTORE_OK && v == 99);
        kvstore_destroy(s);
    }

    printf("[PASS 7] 日志压缩测试成功！\n");
}

// ========  test 8. 异常关闭测试  ========
void test_crash_recovery() {
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
    kvstore* s = kvstore_create(log_path);

    // 3. 验证逻辑
    assert(s != NULL);
    long v;
    assert(kvstore_search(s, 1, &v) == 0 && v == 100);  // key 1 必须存在
    assert(kvstore_search(s, 2, &v) != 0);              // key 2 必须不存在

    kvstore_destroy(s);
    printf("[PASS 8] 异常关闭模拟测试成功!\n");
}

int main() {
    printf("\n========== KVstore V3 综合性能测试 ==========\n");

    test_basic_put_search();
    test_update_replay();
    test_delete_replay();
    test_readonly_lock();
    test_replay();

    test_corruption();
    test_log_compaction();
    test_crash_recovery();
    printf("ALL TESTS PASSED ! \n");
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