
///home/ubuntu/c++2512/KVstore/test/test_kvstore.c
#include <stdio.h>
#include "kvstore.h"

int main() {
    // 创建一个 kvstore 实例
    kvstore* store = kvstore_create();

    // 插入数据
    kvstore_insert(store, 1, 100);
    kvstore_insert(store, 2, 200);
    kvstore_insert(store, 3, 300);

    // 查找数据
    long value;
    if(kvstore_search(store, 2, &value) == 0) {
        printf("Found key 2, value: %ld\n", value);  
    } else {
        printf("Key 2 not found!\n");
    }

    // 删除数据
    kvstore_delete(store, 2);

    // 再次查找数据
    if(kvstore_search(store, 2, &value) != 0) {
        printf("Key 2 not found after deletion.\n");
    } else {
        printf("Delete key 2 failed.");
    }

    // 销毁 KVstore
    kvstore_destroy(store);

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