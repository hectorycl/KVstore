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

#define BPTREE_OK  0
#define BPTREE_UPDATED 1
#define BPTREE_ERR -1

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

// ============= KV 相关接口  ==============
/**
 * 叶子遍历回调函数
 * 返回 0 继续遍历
 * 返回非0 提前终止
 *
 * “协议”（函数指针）
 * 函数指针可以储存“一个函数的地址”
 * int (*visit)(...);  内存里存的是一段指令的起始地址
 *
 * 回调机制（Callback）
 * 本质是：控制权的反转
 * 把compact_visit 函数交给 bptree_scan, bptree_scan 在遍历时，会回过头来调用自己写的那个函数
 *  - 实现了底层(B+ 树) 与业务逻辑（写日志文件）的完全解耦：B+ 树只负责把 key 和 value 从树叶里掏出来，
 *      扔给回调函数，
 *
 * 万能桥梁：void* arg - 万能指针
 *
 * int - 返回值
 * (*bptree_leaf_visit_fn) - 新类型的名字，凡是属于这种类型的变量，本质上都是一个指针
 *                           且它专门指向一类函数
 * (int key, long value, void* arg) - 参数
 *
 * 简化：
 *  - int (*fn)(int, long, void*);  - fn 是一个函数指针
 */
typedef int (*bptree_leaf_visit_fn)(int key, long value, void* arg);
int bptree_scan(bptree* tree, bptree_leaf_visit_fn visit, void* arg);

#ifdef __cplusplus
}
#endif

#endif  // BPTREE_H
