#include "kvstore.h"

#include <stdio.h>
#include <stdlib.h>

// 创建 KVstore
kvstore* kvstore_create() {
    kvstore* store = (kvstore*)malloc(sizeof(kvstore)); // 分配内存
    if(!store) {
        perror("malloc kvstore");
        exit(EXIT_FAILURE);
    }

    store->tree = bptree_create();

    return store;
}


/**
 * 销毁 kvstore 
 *  - 如果只 free(store), store 指针没有了，B+ 树所有节点没有被释放，变成了“无法访问的垃圾” -> 内存泄漏 ！
 *  - bptree_destroy 递归 free B+ 树的每个节点，最后再把 store 结构体占用的内存释放掉
 * 
 * 代码层面的理解：
 *  - 创建一个对象时是由外向内：
 *      sotre = malloc(...)
 *      sotre->tree = bptree_create()
 *  - 销毁时必须严格反序（由外向内）：
 *      bptree_destroy(...)
 *      free(store)
 */
void kvstore_destroy(kvstore* store) {
    if(store) {
        bptree_destroy(store->tree);
        free(store);
    }
}


// 插入数据到 KVstore
int kvstore_insert(kvstore* store, int key, long value) {
    return bptree_insert(store->tree, key, value);
}

// 查找数据
int kvstore_search(kvstore* store, int key, long* value) {
    return bptree_search(store->tree, key, value);
}


// 删除数据
int kvstore_delete(kvstore* store, int key) {
    return bptree_delete(store->tree, key);
}




/**
 * - make : 编译并生成可执行文件
 * - ./test_kvstore : 运行可执行文件
 * - make clean : 清理生成的目标文件和可执行文件
 */