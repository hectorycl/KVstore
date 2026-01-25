// KVstore/include/index/bptree.h
#ifndef BPTREE_H
#define BPTREE_H

/**
 * __cplusplus 是 C++ 自带的一个宏
 *  - 如果当前是 c++ 编译器在编译这段代码，执行下面的语句
 *  - 如果是纯 c 语言编译器，则直接忽略
 * extren "C": 防止名字粉碎
 *  - C: 函数名就是它的唯一标识。比如 bptree_create 编译后在库里就叫 bptree_create
 *  - C++:支持函数重载（Overloading）。为了区分同名但参数不同的函数，C++ 编译器会进行 Name Mangling（名字粉碎/重整）。
 *          比如 bptree_create() 可能会被编译器改成 _Z13bptree_createv
 *  extern "C" 的作用: 告诉 C++ 编译器：“请用 C 语言的方式来处理这部分代码，保持函数原名，不要乱改名”
 * 加上这一段，能保证链接（Linking）时名字能对得上
 */
#ifdef __cplusplus
extern "C" {
#endif

// #define BPTREE_OK  1
// #define BPTREE_ERR 0

typedef struct _bptree bptree;

// ============= 创建 / 销毁 ===============
bptree* bptree_create();
void bptree_destroy(bptree* tree);

// =============== 查找 ==================
int bptree_search(bptree* tree, int key, long* out_value);

// ============== 插入 / 删除  ============
int bptree_insert(bptree* tree, int key, long value);
int bptree_delete(bptree* tree, int key);

// ============= 调试 / 打印 ===============
void bptree_print_leaves(bptree* tree);
void bptree_print_structure(bptree* tree);

#ifdef __cplusplus
}
#endif

#endif  // BPTREE_H
