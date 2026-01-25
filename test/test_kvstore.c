
/// home/ubuntu/c++2512/KVstore/test/test_kvstore.c
#include <stdio.h>
#include <assert.h>
#include "kvstore.h"

int main() {
    printf("\n========== KVstore V2 综合性能测试 ==========\n");

    // 1. 初始化
    kvstore* store = kvstore_create("data.kv");
    long val;

    // 2. 批量插入测试（测试 B+ 树分裂和日志写入）
    printf("\n[测试] 正在批量插入 1000 条数据...\n");
    for (int i = 1; i <= 1000; i++) {
        kvstore_insert(store, i, i * 10);
    }

    // 3. 随机验证
    kvstore_search(store, 500, &val);
    assert(val == 5000);
    printf("[OK] 内存数据验证通过(Key:500, Value: %ld)\n", val);

    // 4. 更新操作测试(同一个 key 插入两次， 模拟更新)
    printf("[测试] 更新 Key 100 的值 为 9999...\n");
    kvstore_insert(store, 100, 9999);
    kvstore_search(store, 100, &val);
    assert(val == 9999);
    printf("[OK] 更新逻辑验证通过。\n");

    // 5. 删除操作测试
    printf("[测试] 删除 Key 500...\n");
    kvstore_delete(store, 500);

    if (kvstore_search(store, 500, &val) != 0) {
        printf("[OK] 删除逻辑验证通过。\n");
    }

    // 6. 正常退出
    kvstore_destroy(store);
    printf("\n===========  阶段一 测试完成，请再次运行以验证持久化\n");

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