// KVstore/src/index/bptree.c

#include "index/bptree.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// ================== 宏定义（唯一来源） ==================
#define ORDER 6
#define MAX_KEYS (ORDER - 1)
#define MAX_CHILDREN ORDER
#define MIN_KEYS ((MAX_KEYS + 1) / 2)

#define MAX_TREE_LEVEL 16  // B+ 树高度极少超过这个树

// 定义一个简单的路径结构，用于记录写路径上持有的锁
typedef struct {
    bptree_node* nodes[MAX_TREE_LEVEL];
    int top;  // 栈顶指针
} bptree_write_path;

// ================== 类型定义（内部） ==================
typedef struct _bptree_node {
    int is_leaf;
    int key_count;
    int keys[MAX_KEYS];
    struct _bptree_node* parent;

    union {
        struct _bptree_node* children[MAX_CHILDREN];
        struct {
            long values[MAX_KEYS];
            struct _bptree_node* next;
        };
    };

    // === V5 ===
    pthread_rwlock_t latch;

} bptree_node;

struct _bptree {
    bptree_node* root;

    // == 入口指针锁 ==
    pthread_mutex_t root_lock;
};

// ================== Internal API ==================
// ========== 插入（内部实现） ==========
static bptree_node* bptree_create_node(int is_leaf);
static int bptree_contains(bptree* tree, int key);
static void bptree_insert_into_leaf(bptree_node* leaf, int key, long value);
static int bptree_split_leaf_and_insert(bptree* tree, bptree_node* leaf, int key, long value, bptree_write_path* path);
static int bptree_insert_into_parent(bptree* tree, bptree_write_path* path, bptree_node* parent, int key, bptree_node* right_child);
static int bptree_split_internal_and_insert(bptree* tree, bptree_write_path* path, bptree_node* node, int key, bptree_node* right_child);
static bptree_node* bptree_find_leaf_write_safe(bptree* tree, int key, bptree_write_path* path);

// ========== 删除（内部实现） ==========
static int bptree_delete_from_leaf(bptree* tree, bptree_node* leaf, int key);
static int bptree_is_underflow(bptree* tree, bptree_node* node);
static bptree_node* bptree_get_left_sibling(bptree_node* node);
static bptree_node* bptree_get_right_sibling(bptree_node* node);
static int bptree_borrow_from_left_leaf(bptree_node* parent, bptree_node* left, bptree_node* leaf, int parent_key_idx);
static int bptree_borrow_from_right_leaf(bptree_node* parent, bptree_node* leaf, bptree_node* right, int parent_key_idx);
static int bptree_borrow_from_left_internal(bptree_node* parent, bptree_node* left, bptree_node* node, int parent_key_idx);
static int bptree_borrow_from_right_internal(bptree_node* parent, bptree_node* node, bptree_node* right, int parent_key_idx);
static int bptree_merge_leaf(bptree* tree, bptree_node* left, bptree_node* right);
static void bptree_merge_internal(bptree_node* parent, bptree_node* left, bptree_node* right, int parent_key_idx);
static int bptree_delete_from_leaf(bptree* tree, bptree_node* leaf, int key);
static void bptree_delete_from_internal(bptree_node* node, int key_idx);
static void bptree_delete_fixup(bptree* tree, bptree_write_path* path, bptree_node* node);
static void bptree_fix_leaf(bptree* tree, bptree_write_path* path, bptree_node* leaf);
static void bptree_fix_internal(bptree* tree, bptree_write_path* path, bptree_node* node);

// ========== 查找（内部） ==========
static bptree_node* bptree_find_leaf(bptree* tree, int key);

// ========== 打印 / 调试 ==========
static void bptree_print_level_bfs(bptree_node* root);

// ========== 销毁（内部） ==========
static void bptree_destroy_node(bptree_node* node);

// =========== KV 相关操作  ===========
static bptree_node* bptree_leafmost_leaf(bptree* tree);
int bptree_scan(bptree* tree, bptree_leaf_visit_fn visit, void* arg);

// ======  并发相关  ========
static void bptree_unlock_all_in_path(bptree_write_path* path);
static bptree_node* bptree_find_leaf_delete_safe(bptree* tree, int key, bptree_write_path* path);

// 创建一个新的 B+ 树节点
bptree_node* bptree_create_node(int is_leaf) {
    bptree_node* node = (bptree_node*)malloc(sizeof(bptree_node));
    if (!node) {
        perror("malloc bptree_node");
        return NULL;  // 返回 NULL, 让上层去处理
    }

    node->is_leaf = is_leaf;
    node->key_count = 0;
    node->parent = NULL;
    node->next = NULL;

    // keys / values
    for (int i = 0; i < MAX_KEYS; i++) {
        node->keys[i] = 0;
        node->values[i] = 0;
    }

    // children
    for (int i = 0; i < MAX_CHILDREN; i++) {
        node->children[i] = NULL;
    }

    // 初始化锁
    if (pthread_rwlock_init(&node->latch, NULL) != 0) {
        free(node);
        return NULL;
    }

    return node;
}

// 创建一个新的 B+ 树
bptree* bptree_create() {
    bptree* tree = (bptree*)malloc(sizeof(bptree));  // 分配 B+ 树结构体内存
    if (tree == NULL) {
        fprintf(stderr, "bptree 内存分配失败！\n");
        exit(EXIT_FAILURE);
    }
    tree->root = bptree_create_node(1);  // 根节点初始一定是叶子

    // 初始化入口锁
    if (pthread_mutex_init(&tree->root_lock, NULL) != 0) {
        free(tree);
        return NULL;
    }

    return tree;
}

// 查找包含指定 key 的叶子节点
bptree_node* bptree_find_leaf(bptree* tree, int key) {
    bptree_node* current = tree->root;
    if (current == NULL) {
        return NULL;
    }

    while (!current->is_leaf) {
        int i = 0;
        // 找到最小的 i，使得 key < keys[i]
        while (i < current->key_count && key >= current->keys[i]) {
            i++;
        }
        current = current->children[i];
    }

    if (current->is_leaf == 0) return NULL;

    return current;
}

/**
 * 悲观写路径查找
 *
 * 核心策略：蟹行锁
 *
 * @param
 *  tree：指向 B+ 树的指针
 *  key : 带插入的键值
 *  path: 用于托管路径锁的栈（外出分配）
 *  bptree_node*: 返回加锁后的叶子节点，失败返回 NULL
 */
static bptree_node* bptree_find_leaf_write_safe(bptree* tree, int key, bptree_write_path* path) {
    if (!tree) return NULL;

    // 重置路径栈
    path->top = 0;

    // 1. 获取全局 root 锁（仅保护 tree->root 指针在缩高/分裂时的原子性）
    pthread_mutex_lock(&tree->root_lock);  // 189 行

    if (tree->root == NULL) {
        pthread_mutex_unlock(&tree->root_lock);  // 必须在 return 前解锁！
        return NULL;
    }

    bptree_node* curr = tree->root;

    // 锁住根节点（只加一次锁！）
    pthread_rwlock_wrlock(&curr->latch);  // 写操作拿锁
    pthread_mutex_unlock(&tree->root_lock);

    // 将根节点入栈
    path->nodes[path->top++] = curr;

    while (!curr->is_leaf) {
        // 查找下一个子节点索引
        int i = 0;
        while (i < curr->key_count && key >= curr->keys[i]) i++;
        bptree_node* next = curr->children[i];

        // 2. 锁住孩子, 并入栈
        pthread_rwlock_wrlock(&next->latch);

        // 先入栈，此时 path 保证了包含当前所有持锁节点
        path->nodes[path->top++] = next;

        // 3. 关键：判断孩子是否安全
        if (next->key_count < MAX_KEYS) {
            // 释放除 next 之外的所有祖先锁
            // 此时栈里有:[root, ..., parent, next]
            // 只需保留刚锁中的这个 next （它是 path->nodes[path->top - 1]）
            for (int j = 0; j < path->top - 1; j++) {
                pthread_rwlock_unlock(&path->nodes[j]->latch);
                path->nodes[j] = NULL;
            }

            // [路径重整]：将 next 设为栈底元素，重置 top
            // 这样 path 栈中就只剩下这一个“安全起始点”
            path->nodes[0] = next;
            path->top = 1;
        }

        curr = next;
    }

    // 此时 curr 是加锁的叶子，path 栈中存放了所有需要保持锁定状态的节点
    return curr;
}

// 在 B+ 树中搜索指定 key 的值,如果找到则将值存入 out_value 并返回 1，否则返回 0
int bptree_search(bptree* tree, int key, long* out_value) {
    // 1. 基础检查
    if (!tree || !tree->root || out_value == NULL) return BPTREE_ERR;

    // 1. [起点] 锁定根节点  -- todo
    pthread_mutex_lock(&tree->root_lock);  // 1. 锁住指针访问
    bptree_node* curr = tree->root;
    pthread_rwlock_rdlock(&curr->latch);     // 2. 锁住节点内容
    pthread_mutex_unlock(&tree->root_lock);  // 3. 释放指针访问（任务完成）

    // 2. [向下遍历] 实现锁蟹行
    while (!curr->is_leaf) {
        int i = 0;

        while (i < curr->key_count && key >= curr->keys[i]) {
            i++;
        }

        bptree_node* next_node = curr->children[i];
        assert(next_node != NULL);

        // ---  蟹行核心代码  ---
        pthread_rwlock_rdlock(&next_node->latch);  // a. 先抓牢孩子（Hold child）
        pthread_rwlock_unlock(&curr->latch);       // b. 再放开家长（Release parent）
        curr = next_node;                          // c. 移动指针
    }

    // 3. [到达叶子] 此时 curr 持有读锁
    int ret = BPTREE_ERR;
    for (int i = 0; i < curr->key_count; i++) {
        if (curr->keys[i] == key) {
            *out_value = curr->values[i];
            ret = BPTREE_OK;
            break;
        } else if (curr->keys[i] > key) {
            break;
        }
    }

    // 4. [收尾] 无论是否找到，最后必须释放叶子节点的锁
    pthread_rwlock_unlock(&curr->latch);

    return ret;
}

/**
 * 是否包含 key, 复用代码，不碰 value
 * 基本用不到了
 */
static __attribute__((unused)) int bptree_contains(bptree* tree, int key) {
    if (tree == NULL || tree->root == NULL)
        return BPTREE_ERR;

    bptree_node* leaf = bptree_find_leaf(tree, key);
    if (leaf == NULL)
        return BPTREE_ERR;

    for (int i = 0; i < leaf->key_count; i++) {
        if (leaf->keys[i] == key)
            return BPTREE_OK;
        if (leaf->keys[i] > key)
            break;
    }
    return BPTREE_ERR;
}

/**
 * 找 child 在 parent 中的 idx
 */
int bptree_find_child_index(bptree_node* parent, bptree_node* child) {
    for (int i = 0; i <= parent->key_count; i++) {
        if (parent->children[i] == child) {
            return i;
        }
    }
    return -1;
}

/**
 * 在叶子节点中插入键值对(假设 leaf 一定有空间),保序
 */
void bptree_insert_into_leaf(bptree_node* leaf, int key, long value) {
    if (leaf == NULL) {
        fprintf(stderr, "叶子结点为空,无法插入！\n");
        return;
    }
    // 不访问 parent

    int idx = 0;
    // 1. 找到插入位置（保持 keys 有序）
    while (idx < leaf->key_count && leaf->keys[idx] < key) {
        idx++;
    }

    // 2. 右移，为新 key 腾出位置
    for (int i = leaf->key_count; i > idx; i--) {
        leaf->keys[i] = leaf->keys[i - 1];
        leaf->values[i] = leaf->values[i - 1];
    }

    // 替换 for 循环的更高效写法  -- todo
    // if (leaf->key_count > idx) {
    //     memmove(&leaf->keys[idx + 1], &leaf->keys[idx], sizeof(int) * (leaf->key_count - idx));
    //     memmove(&leaf->values[idx + 1], &leaf->values[idx], sizeof(long) * (leaf->key_count - idx));
    // }

    // 3. 插入 key / value
    leaf->keys[idx] = key;
    leaf->values[idx] = value;
    leaf->key_count++;

    // printf("[DEBUG] after insert, key_count = %d\n", leaf->key_count);
}

/**
 * 插入键值对 （V5 并发改进版本）
 *
 * 锁协议声明：
 * ENQUIRES: [要求]
 *  - 内部调用 bptree_find_leaf_write 必须通过“锁蟹行”获取目标叶子节点的写锁 （WRLOCK）
 *  - 通过下行遍历获取目标叶子节点的独占锁 （Exclusive Latch）
 *
 * SAFE:
 *  - 若叶子节点仍有空间
 *    插入不会引发结构变化
 *    只需持有当前 leaf 写锁即可完成操作
 *
 * UNSAFE:
 *  - 若节点已满
 *    需要进行 split(结构修改)
 *    当前函数不释放 leaf 锁， 锁所有权转移给 split 函数
 *
 * ENSURES:
 *  - 函数返回前,所有在遍历过程中获取的节点锁必须全部释放（即返回前，不会遗留锁）
 *
 * NOTE:
 *  - 如果发生分裂，叶子节点的锁所有权转移给 bptree_split_leaf_and_insert()
 */
int bptree_insert(bptree* tree, int key, long value) {
    if (tree == NULL || tree->root == NULL) return BPTREE_ERR;

    // 1. 初始化路径栈（用于托管从上至下的 WRLock）
    bptree_write_path path = {.top = 0};

    // 1. 搜寻目标叶子（悲观搜寻）
    // 注意： 此函数内部会根据“安全节点”逻辑提前释放不必要的祖先锁
    bptree_node* leaf = bptree_find_leaf_write_safe(tree, key, &path);
    if (leaf == NULL) {
        // 理论上 不该返回 NULL, 除非 root 流失
        bptree_unlock_all_in_path(&path);
        return BPTREE_ERR;
    }

    // 3. 查找 key 是否已经在叶子节点中
    for (int i = 0; i < leaf->key_count; i++) {  // 可优化：二分查找 todo
        if (leaf->keys[i] == key) {
            leaf->values[i] = value;
            bptree_unlock_all_in_path(&path);  // --- 使用 path 释放路径上所有的锁 ---
            return BPTREE_UPDATED;
        }
        if (leaf->keys[i] > key) break;  // 因为 keys 有序，提前结束
    }

    // 4. 没找到：处理插入与分裂
    int ret = BPTREE_OK;
    if (leaf->key_count < MAX_KEYS) {
        // SAFE: 节点有空间，本次插入不会引起结构变化
        bptree_insert_into_leaf(leaf, key, value);
    } else {
        // UNSAFE：节点已满，插入将触发结构修改（需要分裂）
        // 此时 path 栈中保存了从“最近的 Safe 祖先” 到“叶子”的所有锁链
        // 严禁再次 find_leaf
        ret = bptree_split_leaf_and_insert(tree, leaf, key, value, &path);
    }

    // 5. 统一解锁
    bptree_unlock_all_in_path(&path);
    return ret;
}

/**
 * 分裂叶子节点并插入新的键值对
 * 目标：
 *  1. 将（leaf + 新 key）共 MAX_KEYS + 1 个
 *  2. 左节点保留前 half 个 key
 *  3. 右节点保留后半个 key
 *  4. 将右节点的第一个 key 上推到父节点
 *
 *
 * 并发前置：
 *  - leaf 写锁必须已持有，函数负责释放 leaf 锁
 *
 * 锁协议
 * =========
 * 进入函数时：
 *  - leaf 已持有写锁（由 insert 传入）
 *
 * 本函数：
 *  - 创建并锁 right
 *
 * 向上传播：
 *  - 获取 parent 写锁
 *  - 锁所有权交给 insert_into_parent
 *
 * 根分裂：
 *  - 创建 root 并锁
 *  - 完成本函数释放全部锁
 *
 * ENSURES:
 *  - 返回前释放 leaf / right / parent (若持有) 的所有锁
 */
int bptree_split_leaf_and_insert(bptree* tree, bptree_node* leaf, int key, long value, bptree_write_path* path) {
    // ==  1. 创建右节点 ==
    bptree_node* right = bptree_create_node(1);
    if (!right) return BPTREE_ERR;

    // 将新节点也加入路径栈，以便最后统一释放（虽然它是新出的，但是为了契约一致性）
    // note: 新节点在挂载之前其实不加锁也行，但加了更稳，且能通过 path 自动释放
    pthread_rwlock_wrlock(&right->latch);
    if (path->top < MAX_TREE_LEVEL) {
        path->nodes[path->top++] = right;
    } else {
        // 理论上不会发生，但为了安全
        free(right);  // 假设你有对应的销毁函数
        return BPTREE_ERR;
    }

    // == 2. 分配临时数组 （使用局部变量或栈分配以优化性能） ==
    int total = leaf->key_count + 1;
    int temp_keys[MAX_KEYS + 1];
    long temp_values[MAX_KEYS + 1];

    // == 3. 合并排序 ==
    // 将新 key 插入正确位置，同时迁移旧数据
    int i = 0, j = 0;
    while (i < leaf->key_count && leaf->keys[i] < key) {
        temp_keys[j] = leaf->keys[i];
        temp_values[j] = leaf->values[i];
        i++;
        j++;
    }
    // 放入新 key
    temp_keys[j] = key;
    temp_values[j] = value;
    j++;
    // 迁移剩余数据
    while (i < leaf->key_count) {
        temp_keys[j] = leaf->keys[i];
        temp_values[j] = leaf->values[i];
        i++;
        j++;
    }

    // == 4. 分裂数据 （重新分配 leaf / right 的 key) ==
    int split = (total + 1) / 2;

    leaf->key_count = split;
    for (i = 0; i < split; i++) {
        leaf->keys[i] = temp_keys[i];
        leaf->values[i] = temp_values[i];
    }

    right->key_count = total - split;
    for (i = split, j = 0; i < total; i++, j++) {
        right->keys[j] = temp_keys[i];
        right->values[j] = temp_values[i];
    }

    // == 5. 维护叶链 ==
    right->next = leaf->next;
    leaf->next = right;
    right->parent = leaf->parent;

    // == 6. 向父传播  ==
    int promote_key = right->keys[0];

    // ROOT SPLIT
    if (leaf->parent == NULL) {
        //[根分裂]
        bptree_node* new_root = bptree_create_node(0);
        if (!new_root) {
            // 注意：这里由于外层 bptree_insert 还会调用 unlock_all_in_path，
            // 所以这里 return ERR 是安全的，锁会被外层解开。
            return BPTREE_ERR;
        }

        new_root->keys[0] = promote_key;
        new_root->children[0] = leaf;
        new_root->children[1] = right;
        new_root->key_count = 1;
        new_root->is_leaf = 0;

        leaf->parent = new_root;
        right->parent = new_root;

        pthread_mutex_lock(&tree->root_lock);  // 修改前必须加锁
        tree->root = new_root;
        pthread_mutex_unlock(&tree->root_lock);

        return BPTREE_OK;
    }

    // [向上传播]
    // 关键：parent 已经在 path 栈里被锁住了，不需要重新加锁！
    bptree_node* parent = leaf->parent;

    // 不再需要 find_child_index,因为在 internal 插入中会重新根据 key 定位
    return bptree_insert_into_parent(tree, path, parent, promote_key, right);
}

/**
 * 父节点插入函数(父节点一定不为 NULL)
 * 1. 取出 node 的父节点 parent
 * 2. parent 以及右侧的叔父节点右移动一位，腾出位置
 * 3. right 的 keys[0] 上移、插入 right 到 parent
 * 4. 更新 key_count
 *
 * 锁协议：
 *  - parent 已持写锁
 *  - right_chlid 已持写锁
 *
 * SAFE:
 *    本函数负责完成插入并释放 right_child 锁
 *    parent 锁由调用者决定是否释放
 *
 * UNSAFE:
 *    锁所有权转移给 split 函数
 *    split 负责后续所有解锁
 *
 */
int bptree_insert_into_parent(bptree* tree, bptree_write_path* path, bptree_node* parent, int key, bptree_node* right_child) {
    if (!tree || !parent || !right_child) return BPTREE_ERR;

    // 1. 自动寻找插入位置
    // 在父节点中找到第一个大于等于 key 的位置
    int insert_index = 0;
    while (insert_index < parent->key_count && parent->keys[insert_index] < key) {
        insert_index++;
    }

    // === 情况A: 父节点有空间 (SAFE) ===
    if (parent->key_count < MAX_KEYS) {
        // 右移 key
        for (int i = parent->key_count; i > insert_index; i--) {
            parent->keys[i] = parent->keys[i - 1];
        }

        // 右移 child 指针
        for (int i = parent->key_count + 1; i > insert_index + 1; i--) {
            parent->children[i] = parent->children[i - 1];
        }

        // 插入新 key 和 新孩子
        parent->keys[insert_index] = key;
        parent->children[insert_index + 1] = right_child;
        right_child->parent = parent;

        parent->key_count++;

        // 成功插入，无需进一步结构调整
        // 锁依然由最外层的 bptree_insert 统一通过 path 释放
        return BPTREE_OK;
    }

    // === 情况B: 父节点也满了（UNSAFE） ===
    // 触发内部节点的分裂，同样将 path 传下去
    return bptree_split_internal_and_insert(tree, path, parent, key, right_child);
}

/**
 * 分裂内部节点并插入新的键值对（共 MAX_KEYS + 1 个）
 * 目标：
 *  1. 创建 right_internal
 *  2. 选出 middle index
 *  3. middle key = left->keys[middle]
 *  4. 左边保留 [0, mid - 1]
 *  5. 右边拷贝 [mid + 1, key_count -1]  ---> 内部节点分裂后，middle_key 不属于任何一边，middle_key 上升
 *  6. 处理 children 的 parent 指针
 *  7. 如果 left->parent == NULL -> 新建根
 *  8. 否则递归 insert_into_parent(parent, left, right_internal, middle_key)
 *
 * split 不再依赖 parent 当前状态
 *
 *
 *
 * 锁协议
 * ============
 *
 * NOTE: 谁拥有所有权，谁就负责在任务结束时调用 pthread_rwlock_unlock
 *
 * 进入函数时：
 *   node         - 已持写锁
 *   right_child  - 已持写锁
 *
 * 本函数负责：
 *   创建并锁定 right 节点
 *   重新分布 children parent 指针
 *
 * SAFE（root split）:
 *   创建新 root 并加锁
 *   更新 tree->root
 *   释放： node, right, right_child
 *
 *
 * UNSAFE(向上传播)：
 *   获取 parent 写锁
 *   调用 insert_into_parent
 *   锁所有权转移给上层
 *
 *
 */
int bptree_split_internal_and_insert(bptree* tree, bptree_write_path* path, bptree_node* node, int key, bptree_node* right_child) {
    // 1. 临时数组：keys + children
    int total_keys = node->key_count + 1;
    int temp_keys[MAX_KEYS + 1];
    bptree_node* temp_children[MAX_KEYS + 2];

    // 自动寻找插入位置
    int insert_index = 0;
    while (insert_index < node->key_count && node->keys[insert_index] < key) {
        insert_index++;
    }

    // === 2. 将数据填充到临时缓冲区 ===
    int i, j;
    // 填充 keys
    for (i = 0, j = 0; i < node->key_count; i++, j++) {
        if (j == insert_index) j++;  // 跳过插入点
        temp_keys[j] = node->keys[i];
    }
    temp_keys[insert_index] = key;

    // 填充 children
    // 原有的 children 保持相对顺序，right_child 插入到 insert_idx + 1
    for (i = 0, j = 0; i <= node->key_count; i++, j++) {
        if (j == insert_index + 1) j++;
        temp_children[j] = node->children[i];
    }
    temp_children[insert_index + 1] = right_child;

    //right_child->parent = node;

    // === 3. 确定分裂点（提拔中间的 Key） ===
    int mid = total_keys / 2;
    int middle_key = temp_keys[mid];

    // === 4. 创建右节点(新创建节点暂不加入 path, 因为它还没有被树引用) ===
    bptree_node* right = bptree_create_node(0);
    if (!right) return BPTREE_ERR;

    // 锁住新节点并加入 path 托管
    pthread_rwlock_wrlock(&right->latch);
    if (path->top < MAX_TREE_LEVEL) {
        path->nodes[path->top++] = right;
    } else {
        pthread_rwlock_unlock(&right->latch); // 必须解开，否则该节点永远被锁死
    }

    right->parent = node->parent;

    // === 5. 重新填充原节点（左半部分） ===
    node->key_count = mid;
    for (int i = 0; i <= mid; i++) {
        node->children[i] = temp_children[i];
        if (node->children[i]) {
            node->children[i]->parent = node; // 确保左侧所有孩子指向 node
        }
        if (i < mid) {
            node->keys[i] = temp_keys[i];
        }
    }

    // === 6. 填充新节点（右半部分） ===
    right->key_count = total_keys - mid - 1;
    for (i = mid + 1, j = 0; i < total_keys; i++, j++) {
        right->keys[j] = temp_keys[i];
        right->children[j] = temp_children[i];
        if (right->children[j]) {
            right->children[j]->parent = right; // 确保右侧所有孩子指向 right
        }
    }
    // 处理右节点的最后一个孩子
    right->children[right->key_count] = temp_children[total_keys];
    if (right->children[right->key_count]) {
        right->children[right->key_count]->parent = right;
    }

    // 7. 根分裂 - 处理向上递归
    if (node->parent == NULL) {
        // 根分裂：创建新根
        bptree_node* new_root = bptree_create_node(0);
        if (!new_root) return BPTREE_ERR;

        new_root->keys[0] = middle_key;
        new_root->children[0] = node;
        new_root->children[1] = right;
        new_root->key_count = 1;

        node->parent = new_root;
        right->parent = new_root;

        pthread_mutex_lock(&tree->root_lock);
        tree->root = new_root;
        pthread_mutex_unlock(&tree->root_lock);

        return BPTREE_OK;
    }

    // 递归向上：父节点已经在 path 中被锁住
    return bptree_insert_into_parent(tree, path, node->parent, middle_key, right);
}

/**
 * 判断节点是否低于最小 key 数量(下溢)
 *
 * @return
 *  - BPTREE_ERR: 正常，没有下溢
 *  - BPTREE_OK: 发生下溢
 *
 */
int bptree_is_underflow(bptree* tree, bptree_node* node) {
    if (node == tree->root)
        return BPTREE_ERR;

    // #define MIN_KEYS ((ORDER + 1) / 2 - 1)
    if (node->key_count < MIN_KEYS) {
        return BPTREE_OK;
    }

    return BPTREE_ERR;
}

// 兄弟节点定位
/**
 * 找到 node 的左兄弟（同父）
 * 不存在返回 NULL
 */
static __attribute__((unused))
bptree_node*
bptree_get_left_sibling(bptree_node* node) {
    if (node == NULL || node->parent == NULL) {
        return NULL;
    }

    int idx = bptree_find_child_index(node->parent, node);
    if (idx == 0 || idx == -1) {
        return NULL;
    }

    // idx >= 1
    return node->parent->children[idx - 1];
}

/**
 * 找到 node 的右兄弟（同父）
 * 不存在返回 NULL
 */
static __attribute__((unused))
bptree_node*
bptree_get_right_sibling(bptree_node* node) {
    if (node == NULL || node->parent == NULL) {
        return NULL;
    }

    int idx = bptree_find_child_index(node->parent, node);
    if (idx == node->parent->key_count || idx == -1) {
        return NULL;
    }

    // idx < node->key_count
    return node->parent->children[idx + 1];
}

/**
 * 找左兄弟借一个 key
 *  借到了：BPTREE_OK
 *  失败(左兄弟不够)：BPTREE_ERR
 *
 * 参数：
 *  parent: 父节点（已加锁）
 *  left: 左兄弟（已加锁）
 *  leaf：当前下溢的叶子（已加锁）
 *  parent_key_idx: parent->keys 中分割 left 和 leaf 的那个 key 的索引
 */
int bptree_borrow_from_left_leaf(bptree_node* parent, bptree_node* left, bptree_node* leaf, int parent_key_idx) {
    // 1. 基础安全检查（虽然理论上由 fix_leaf 保证）
    if (left->key_count <= MIN_KEYS) {
        return BPTREE_ERR;  // 左兄弟也不够借
    }

    // 2. 给 leaf 腾出首位（index 0）
    // 原有的 keys[0...count-1] 向后移动到 keys[1...count]
    if (leaf->key_count > 0) {
        memmove(&leaf->keys[1], &leaf->keys[0], sizeof(int) * leaf->key_count);
        memmove(&leaf->values[1], &leaf->values[0], sizeof(long) * leaf->key_count);
    }

    // 3. 转移：将左兄弟的最后一个键值对，移到当前叶子的第一个位置
    int last_idx_of_left = left->key_count - 1;
    leaf->keys[0] = left->keys[last_idx_of_left];
    leaf->values[0] = left->values[last_idx_of_left];

    // 4. 更新父节点：
    // 叶子节点借键后，父节点中指向右侧孩子（也就是当前 leaf）的键需要更新为新的首位键
    parent->keys[parent_key_idx] = leaf->keys[0];

    // 5. 清理左兄弟末尾脏数据（可选）
    left->keys[last_idx_of_left] = 0;
    left->values[last_idx_of_left] = 0;

    // 6. 更新计数
    left->key_count--;
    leaf->key_count++;

    return BPTREE_OK;
}

/**
 * 找右兄弟借一个 key
 *  接到了： BPTREE_OK
 *  失败： BPTREE_ERR
 *
 */
int bptree_borrow_from_right_leaf(bptree_node* parent, bptree_node* leaf, bptree_node* right, int parent_key_idx) {
    if (right->key_count <= MIN_KEYS) return BPTREE_ERR;

    // 1. 转移：把右兄弟的第一个移到当前叶子的末尾
    leaf->keys[leaf->key_count] = right->keys[0];
    leaf->values[leaf->key_count] = right->values[0];

    // 2. 更新父节点：
    // 右侧兄弟失去了它的第一个 key, 所以父节点中对应的 key 要更新为右兄弟新的第一个 key
    parent->keys[parent_key_idx] = right->keys[1];

    // 3. 右兄弟数据前移
    int num_to_move = right->key_count - 1;
    memmove(&right->keys[0], &right->keys[1], sizeof(int) * num_to_move);
    memmove(&right->values[0], &right->values[1], sizeof(long) * num_to_move);

    // 4. 更新计数
    leaf->key_count++;
    leaf->key_count--;

    return BPTREE_OK;
}

/**
 * 内部节点向左兄弟借键 (Action Layer)
 *
 * 使用前提：
 *  - 保证 node 需要借(node 一定是 underflow): node->key_count < MAX_KEYS
 *
 * 旋转规则：
 *  1. parent->keys[parent_key_idx] 下沉到 node->keys[0]
 *  2. left->keys[last] 上提到 parent->keys[parent_key_idx]
 *  3. left->children[last] 移动到 node->children[0]
 *
 * @return
 *  - BPTREE_OK   借成功，结构已修复
 *  - BPTREER_ERR 借失败（左兄弟不存在或者 key 不足）
 */
static int bptree_borrow_from_left_internal(bptree_node* parent, bptree_node* left, bptree_node* node, int parent_key_idx) {
    // 1. 安全检查（理论上由 fix_internal 保证）
    if (left->key_count <= MIN_KEYS) return BPTREE_ERR;

    // 2. 为 node 腾出首位空间 (Keys 和 Children)
    // Keys 右移 1 位
    memmove(&node->keys[1], &node->keys[0], sizeof(int) * node->key_count);
    // Children 右移 1 位 (注意：内部节点的孩子数是 key_count + 1)
    memmove(&node->children[1], &node->children[0], sizeof(bptree_node*) * (node->key_count + 1));

    // 3. 执行旋转动作
    // A. 父节点键下沉：parent 的分界键进入 node 的第一个位置
    node->keys[0] = parent->keys[parent_key_idx];

    // B. 左兄弟的孩子转移：left 的最后一个孩子变成 node 的第一个孩子
    node->children[0] = left->children[left->key_count];
    if (node->children[0]) {
        node->children[0]->parent = node;  // 维护父指针
    }

    // C. 左兄弟键上提：left 的最后一个键提升到 parent，顶替刚才下沉的键
    parent->keys[parent_key_idx] = left->keys[left->key_count - 1];

    // 4. 清理左兄弟残留 (可选但建议)
    left->keys[left->key_count - 1] = 0;
    left->children[left->key_count] = NULL;

    // 5. 更新计数
    left->key_count--;
    node->key_count++;

    return BPTREE_OK;
}
/**
 * 从左兄弟借一个 key + child (内部节点)
 *
 * 使用前提：
 *   - 保证 node 需要借(node 需要一定是下溢)，node->key_count < MAX_KEYS
 *
 * 旋转规则：
 *  - parent 的分隔 key 下移到 node
 *  - right 的最小 key 上移到 parent
 *  - right 的最左 child 移动到 node 的最右侧
 *
 * @return
 *  - BPTREE_OK  : 借成功，结构已修复
 *  - BPTREE_ERR : 失败（right不存在或者 key 不足）
 */
int bptree_borrow_from_right_internal(bptree_node* parent, bptree_node* node, bptree_node* right, int parent_key_idx) {
    if (right->key_count <= MIN_KEYS) return BPTREE_ERR;

    // 1. 父节点键下沉到 node 末尾
    node->keys[node->key_count] = parent->keys[parent_key_idx];

    // 2. 右兄弟的第一个孩子转给 node
    node->children[node->key_count + 1] = right->children[0];
    if (node->children[node->key_count + 1]) {
        node->children[node->key_count + 1]->parent = node;
    }

    // 3. 右兄弟第一个键上提到 parent
    parent->keys[parent_key_idx] = right->keys[0];

    // 4. 右兄弟数据前移
    memmove(&right->keys[0], &right->keys[1], sizeof(int) * (right->key_count - 1));
    memmove(&right->children[0], &right->children[1], sizeof(bptree_node*) * right->key_count);

    // 5. 更新计数
    node->key_count++;
    right->key_count--;

    return BPTREE_OK;
}

/**
 * 合并 left 和它紧邻的右兄弟（Action Layer）
 * 将 right 的内容合并进 left, 并更新链表指针
 *
 * 注意：此函数不处理 parent 数组的删除，也不释放内存，只负责数据搬运
 *
 * 说明：
 *  - 两个节点合并，一左一右，传进来的为 left
 *  - 合并后的的节点为 left
 *
 * @return
 *  - BPTREE_OK : 合成成功
 *  - BPTREE_ERR: 合并失败
 */

static int bptree_merge_leaf(bptree* tree, bptree_node* left, bptree_node* right) {
    if (!left || !left->parent) return BPTREE_ERR;

    // 1. 容量安全检查 (Double Check)
    if (left->key_count + right->key_count > MAX_KEYS) {
        return BPTREE_ERR;
    }

    // 2. 拷贝数据：使用 memmove/memcpy 提高效率
    int dest_offset = left->key_count;
    int num_to_copy = right->key_count;

    memcpy(&left->keys[dest_offset], &right->keys[0], sizeof(int) * num_to_copy);
    memcpy(&left->values[dest_offset], &right->values[0], sizeof(long) * num_to_copy);

    // 3. 维护叶子层链表（保证范围查询的正确性）
    // 此时 left 和 right 都被锁住，修改 next 指针是线程安全的
    left->next = right->next;

    // 4. 更新计数
    left->key_count += right->key_count;

    return BPTREE_OK;
}

/**
 * 合并两个内部节点 (Action Layer)
 *
 * 规则：(将 parent 的分界键下沉，然后把 right 的键和孩子全部并入 left)
 *  - parent 的 key 下沉
 *  - right 的 key + child 接到 left 后面
 *  - 更新 child->parent
 *  - parent 中删除分隔的 key & right child
 *  - free(right)
 *
 * * 参数：
 * - parent: 父节点（已锁）
 * - left:   左侧内部节点（已锁）
 * - right:  右侧内部节点（已锁）
 * - parent_key_idx: parent->keys 中那个需要“下沉”的键的索引
 */
static void bptree_merge_internal(bptree_node* parent, bptree_node* left, bptree_node* right, int parent_key_idx) {
    int old_left_count = left->key_count;

    // 1. 【核心】父节点键下沉
    // 父节点的 keys[parent_key_idx] 降级为 left 的一个键
    left->keys[old_left_count] = parent->keys[parent_key_idx];

    // 2. 拷贝右兄弟的键
    // 注意：起始位置是 old_left_count + 1
    if (right->key_count > 0) {
        memcpy(&left->keys[old_left_count + 1], right->keys,
               sizeof(int) * right->key_count);
    }

    // 3. 拷贝右兄弟的孩子指针
    // 内部节点有 key_count + 1 个孩子
    memcpy(&left->children[old_left_count + 1], right->children,
           sizeof(bptree_node*) * (right->key_count + 1));

    // 4. 更新搬过来的盖子们的 parent 指针
    for (int i = 0; i <= right->key_count; i++) {
        if (left->children[old_left_count + 1 + i]) {
            left->children[old_left_count + 1 + i]->parent = left;
        }
    }

    // 5. 更新计数: 新计数 = 原左 + 1（下沉键）+ 原右
    left->key_count += (right->key_count + 1);
}

/**
 * B+ 树删除 ≠ BST 删除
它至少包含 4 个子能力：
    在叶子中删除 key
    删除后是否 低于最小 key 数
    不够 → 向兄弟借
    借不到 → 合并 + 父节点删除 key（递归）

所以 bptree_delete() 一定是一个“调度函数”
 */

/**
 * 从叶子中删除 key，以及对应的 value(假设传过来的是叶子节点)
 *
 * @return
 *  - BPTREE_OK : 删除成功
 *  - BPTREE_ERR: 删除失败
 */
int bptree_delete_from_leaf(bptree* tree, bptree_node* leaf, int key) {
    if (!leaf) {
        fprintf(stderr, "叶子节点为空，删除失败\n");
        return BPTREE_ERR;
    }

    // 1. 找到位置  -- 二分优化 todo
    int idx;
    for (idx = 0; idx < leaf->key_count; idx++) {
        if (leaf->keys[idx] == key) {
            break;
        }
    }

    // 2. 若没找到
    if (idx == leaf->key_count) {
        printf("key %d 不存在！\n", key);
        return BPTREE_NOT_FOUND;  // todo
    }

    // 3. 计算需要移动的元素数量
    int num_to_move = leaf->key_count - 1 - idx;
    if (num_to_move > 0) {
        // 使用 memmove 将 idx 之后的所有元素向前挪动一位
        // 参数：目标地址, 源地址, 移动的总字节数
        memmove(&leaf->keys[idx], &leaf->keys[idx + 1], sizeof(int) * num_to_move);
        memmove(&leaf->values[idx], &leaf->values[idx + 1], sizeof(long) * num_to_move);
    }

    // // 3. 覆盖
    // for (int i = idx; i < leaf->key_count - 1; i++) {
    //     leaf->keys[i] = leaf->keys[i + 1];
    //     leaf->values[i] = leaf->values[i + 1];
    // }

    // 4. 清理末尾（逻辑残留清理）并更新计数
    leaf->keys[leaf->key_count - 1] = 0;
    leaf->values[leaf->key_count - 1] = 0;
    leaf->key_count--;

    return BPTREE_OK;
}

// 父节点删除（递归核心）, 是对“父节点 / 内部节点”的结构修复
/**
 * 真难
 * 从内部节点删除一个 key & child（可能触发递归合并）
 *
 * 规则：
 *  - 删除 key & child
 *  - 是否下溢：
 *      - borrow_from_internal
 *      - merge -> 继续对 parent 调用 delete_from_internal (递归)
 *  - 特判 root 缩高
 *
 * 注意：key_index != child_index (key_idx = child_idx - 1)
 *
 *
 * 从内部节点删除指定 key 和它右侧的 child 指针
 * 仅执行数组位移，不涉及锁的获取或递归修复
 */

static void bptree_delete_from_internal(bptree_node* node, int key_idx) {
    if (!node || key_idx < 0 || key_idx >= node->key_count) return;

    // 1. 挪动 keys
    // 要删除 node->keys[key_idx]
    int num_keys_to_move = node->key_count - 1 - key_idx;
    if (num_keys_to_move > 0) {
        memmove(&node->keys[key_idx], &node->keys[key_idx + 1], sizeof(int) * num_keys_to_move);
    }

    // 2. 挪动 children
    // 在 B+ 树合并中，通常是删除 key 及其右侧对应的 child 指针（child_idx = key_idx + 1）
    int child_idx_to_remove = key_idx + 1;
    int num_children_to_move = node->key_count - child_idx_to_remove;
    if (num_children_to_move > 0) {
        memmove(&node->children[child_idx_to_remove], &node->children[child_idx_to_remove + 1],
                sizeof(bptree_node*) * num_children_to_move);
    }

    // 3. 清理末尾并更新计数器
    node->key_count--;
    node->keys[node->key_count] = 0;
    node->children[node->key_count + 1] = NULL;
}

/**
 * 修复 leaf 结构
 *
 * 步骤：
 *  - 1. 借左/右
 *  - 2. 无法借时，merge
 *  - 3. merge 后递归修复父节点
 */
void bptree_fix_leaf(bptree* tree, bptree_write_path* path, bptree_node* leaf) {
    if (path->top <= 0) return;  // 到达根节点，无需修复

    // 1. 从 path 栈获取父节点（此时父节点被 wrlock 锁住）
    bptree_node* parent = path->nodes[path->top - 1];
    int idx = bptree_find_child_index(parent, leaf);

    // 2. 尝试从左兄弟借
    if (idx > 0) {
        bptree_node* left = parent->children[idx - 1];
        pthread_rwlock_wrlock(&left->latch);  // 锁住左兄弟
        if (left->key_count > MIN_KEYS) {
            // 参数：parent, 左兄弟, 当前叶子, 分割它们的 key 索引 (idx-1)
            bptree_borrow_from_left_leaf(parent, left, leaf, idx - 1);
            pthread_rwlock_unlock(&left->latch);
            return;
        }
        pthread_rwlock_unlock(&left->latch);
    }

    // 3. 尝试从右兄弟借
    if (idx < parent->key_count) {
        bptree_node* right = parent->children[idx + 1];
        pthread_rwlock_wrlock(&right->latch);

        if (right->key_count > MIN_KEYS) {
            // 参数：parent, 当前叶子, 右兄弟, 分割它们的 key 索引 (idx)
            bptree_borrow_from_right_leaf(parent, leaf, right, idx);
            pthread_rwlock_unlock(&right->latch);
            return;
        }
        pthread_rwlock_unlock(&right->latch);
    }

    // 4. 借不到则进行合并（Merge）
    if (idx > 0) {
        // 有左兄弟：把 leaf 合并进 left
        bptree_node* left = parent->children[idx - 1];
        pthread_rwlock_wrlock(&left->latch);

        bptree_merge_leaf(tree, left, leaf);           // 合并到 left, leaf 被废弃
        bptree_delete_from_internal(parent, idx - 1);  // // 从父节点删除 keys[idx-1] 和 children[idx]

        pthread_rwlock_unlock(&left->latch);

        // 物理销毁被吞噬的 leaf
        pthread_rwlock_destroy(&leaf->latch);
        free(leaf);
    } else {
        // 只有右兄弟：把 right 合并进 leaf
        bptree_node* right = parent->children[idx + 1];
        pthread_rwlock_wrlock(&right->latch);

        bptree_merge_leaf(tree, leaf, right);    // 合并到 leaf, right 被废弃
        bptree_delete_from_internal(parent, 0);  // 从父节点删除 keys[0] 和 children[1]

        pthread_rwlock_unlock(&right->latch);

        // 物理销毁被吞噬的 right
        pthread_rwlock_destroy(&right->latch);
        free(right);
    }

    // 5. 递归修复父节点
    // 如果 parent 发生了下溢，继续向上回溯
    if (parent != tree->root && bptree_is_underflow(tree, parent)) {
        bptree_delete_fixup(tree, path, parent);
    }
}

/**
 * 删除后内部节点修复逻辑（Borrow / Merge / 向上传递）
 *
 * 锁前提：
 *  - 进入本函数时：
 *      parent → WRLock
 *      node   → WRLock
 *
 * 锁顺序保证：
 *  - 始终保证 parent → node → sibling
 *  - 绝不向上重新加锁（依赖 path 栈托管）
 *
 * 修复流程：
 *  1. 优先尝试从左 / 右兄弟借键（局部修复）
 *  2. 若无法借键，则执行 merge
 *  3. merge 后若 parent 下溢，递归调用 delete_fixup
 *
 */
void bptree_fix_internal(bptree* tree, bptree_write_path* path, bptree_node* node) {
    if (path->top <= 0) return;  // 到达根节点，无需修复

    // 1. 从 path 栈获取已经加锁的父节点
    bptree_node* parent = path->nodes[path->top - 1];
    int idx = bptree_find_child_index(parent, node);

    // 2. 尝试从左兄弟借
    if (idx > 0) {
        bptree_node* left = parent->children[idx - 1];
        pthread_rwlock_wrlock(&left->latch);
        if (left->key_count > MAX_KEYS / 2) {
            bptree_borrow_from_left_internal(parent, left, node, idx - 1);
            pthread_rwlock_unlock(&left->latch);
            return;
        }
        pthread_rwlock_unlock(&left->latch);
    }

    // 3. 尝试从右兄弟借
    if (idx < parent->key_count) {
        bptree_node* right = parent->children[idx + 1];
        pthread_rwlock_wrlock(&right->latch);
        if (right->key_count > MAX_KEYS / 2) {
            bptree_borrow_from_right_internal(parent, node, right, idx);
            pthread_rwlock_unlock(&right->latch);
            return;
        }
        pthread_rwlock_unlock(&right->latch);
    }

    // 4. 借不到则执行合并（Merge）
    if (idx > 0) {
        // 与左兄弟合并：node 被并入 left
        bptree_node* left = parent->children[idx - 1];
        pthread_rwlock_wrlock(&left->latch);

        bptree_merge_internal(parent, left, node, idx - 1);
        bptree_delete_from_internal(parent, idx - 1);  // 从父节点摘除 node

        pthread_rwlock_unlock(&left->latch);

        // 安全销毁 node
        // 此处 free(node),要确保 free 之后不能让 unlock_all_in_path 尝试去解这个锁
        pthread_rwlock_unlock(&node->latch);
        pthread_rwlock_destroy(&node->latch);
        free(node);
    } else {
        // 与右兄弟合并：right 被并入 node
        bptree_node* right = parent->children[idx + 1];
        pthread_rwlock_wrlock(&right->latch);

        bptree_merge_internal(parent, node, right, idx);
        bptree_delete_from_internal(parent, idx);  // 从父节点摘除 right

        pthread_rwlock_unlock(&right->latch);
        pthread_rwlock_destroy(&right->latch);
        free(right);
    }

    // 5. 递归向上修复
    // 注意：root 缩高的逻辑我们已经统一放在 bptree_delete 的末尾处理了，这里只需处理普通下溢
    if (parent != tree->root && bptree_is_underflow(tree, parent)) {
        bptree_delete_fixup(tree, path, parent);
    }
}

/**
 * 删除后结构修复调度器（borrow / merge / 递归向上）
 *
 * 锁协议：
 *  - 进入本函数时：
 *      node 及其 parent 均已持有 WRLock
 *      path 栈中保存了从“最后一个不安全祖先”到 node 的所有 WRLock
 *
 *  - 本函数绝不重新加锁祖先节点
 *    而是通过 path 栈逐级弹出节点，实现“锁托管向上传递”
 *
 * 执行流程：
 *  1. 若 node 是 root → 停止（root 收缩由 delete() 统一处理）
 *  2. 弹出 node, 使 path 栈顶变为 parent
 *  3. 根据节点类型分流：
 *      leaf   → bptree_fix_leaf
 *      internal → bptree_fix_internal
 *
 *
 * 递归策略：
 *  - 若 merge 导致 parent 下溢，fix_internal 会再次调用本函数
 *  - 整个递归过程不发生新的向上加锁
 *
 */
void bptree_delete_fixup(bptree* tree, bptree_write_path* path, bptree_node* node) {
    // 1. 安全边界检查
    if (node == tree->root || path->top <= 0) {
        return;
    }

    // 2. 【关键】弹栈逻辑
    // 在进入此函数前，path->nodes[path->top-1] 指向的是 node 本身。
    // 为了让 fix_leaf/internal 能够通过 path->nodes[path->top-1] 访问到 parent，
    // 必须在这里弹出 node。
    if (path->nodes[path->top - 1] == node) {
        path->top--;
    }

    // 3. 根据节点类型分流
    if (node->is_leaf) {
        // 修复叶子：内部会用到 path 栈顶的 parent
        bptree_fix_leaf(tree, path, node);
    } else {
        // 修复内部节点：内部也会用到 path 栈顶的 parent，并可能递归调用本函数
        bptree_fix_internal(tree, path, node);
    }
}

/**
 * 并发删除专用查找函数（悲观蟹行加锁）
 *
 * 锁协议：
 *  1. 先获取 tree->root_lock, 安全读取 root 指针
 *  2. 对 root 获取 WRLock
 *  3. 释放 root_lock(之后只依赖节点 latch)
 *  4. 自上而下执行 crabbing:
 *      - 先锁 child
 *      - 若 child 是 SAFE(删除后不会下溢)
 *            → 释放路径中所有祖先锁
 *      - 否则继续持有祖先锁
 *
 * SAFE 判定条件（删除场景）：child->key_count > MIN_KEYS
 *
 * 返回时保证：
 *    path 栈中只保留“最后一个不安全节点”到 leaf 的 WRLOCK
 *    当前线程持有这些节点的 WRLock
 *
 * 该函数不会向上加锁，因此不会死锁。
 */
static bptree_node* bptree_find_leaf_delete_safe(bptree* tree, int key, bptree_write_path* path) {
    pthread_mutex_lock(&tree->root_lock);
    bptree_node* curr = tree->root;
    pthread_rwlock_wrlock(&curr->latch);
    pthread_mutex_unlock(&tree->root_lock);

    path->nodes[path->top++] = curr;

    while (!curr->is_leaf) {
        int i = 0;
        while (i < curr->key_count && key >= curr->keys[i]) i++;

        bptree_node* next = curr->children[i];

        // 锁 child (始终保持 parent -> child 顺序)
        pthread_rwlock_wrlock(&next->latch);

        // 删除场景 SAFE 判断
        if (next->key_count > MIN_KEYS) {
            // child 安全 → 释放所有祖先锁
            bptree_unlock_all_in_path(path);
        }

        path->nodes[path->top++] = next;
        curr = next;
    }

    return curr;
}

// 删除接口（指挥官）- 判断、调度、决策下一步调用谁
/**
 * todo
 * 它应该只做 5 件事：
 *  1. find_leaf
 *  2. delete_from_leaf
 *  3. 若未 underflow → return
 *  4. 尝试 borrow（左 / 右）
 *  5. borrow 失败 → merge → 递归向上
 *
 * 只修复了叶子层
 */
int bptree_delete(bptree* tree, int key) {
    if (!tree || !tree->root) return BPTREE_ERR;

    bptree_write_path path = {.top = 0};

    // 1. 悲观加锁查找叶子
    bptree_node* leaf = bptree_find_leaf_delete_safe(tree, key, &path);

    if (leaf == NULL) {
        bptree_unlock_all_in_path(&path);
        return BPTREE_NOT_FOUND;  // 返回明确的不存在状态
    }

    // 2. 叶子删除
    int ret = bptree_delete_from_leaf(tree, leaf, key);
    if (ret != BPTREE_OK) {  // [改动2] 如果没删掉（Key不存在），直接解锁并退出
        bptree_unlock_all_in_path(&path);
        return ret;
    }

    // 3. 如果叶子下溢，修复树（此时 path 栈顶是 leaf）
    if (leaf != tree->root && bptree_is_underflow(tree, leaf)) {
        bptree_delete_fixup(tree, &path, leaf);
    }

    // 4. 根节点收缩特判
    // 注意：这里需要再次检查 root，因为递归修复可能已经把 root 变空了
    // 逻辑：如果原来的 root 变成空的 internal node，则要把它的第一个孩子提升为新 root

    pthread_mutex_lock(&tree->root_lock);
    if (tree->root->key_count == 0 && !tree->root->is_leaf) {
        bptree_node* old_root = tree->root;

        // 提升孩子
        tree->root = old_root->children[0];
        if (tree->root) tree->root->parent = NULL;

        // 【重要改动】：安全解锁
        // 不能直接断定 path.nodes[0] 就是 old_root
        // 应该遍历整个 path 栈，如果 old_root 在里面，就置为 NULL
        for (int i = 0; i < path.top; i++) {
            if (path.nodes[i] == old_root) {
                pthread_rwlock_unlock(&old_root->latch);
                path.nodes[i] = NULL;
                break;
            }
        }

        pthread_rwlock_destroy(&old_root->latch);
        free(old_root);
    }
    pthread_mutex_unlock(&tree->root_lock);

    // 5. 统一释放残留锁
    bptree_unlock_all_in_path(&path);

    return BPTREE_OK;
}

// ===========  打印 B+ 树  ==========
/**
 * 打印叶子节点的 key & value
 */
void bptree_print_leaves(bptree* tree) {
    if (tree == NULL || tree->root == NULL) {
        printf("B+ 树为空!\n");
        return;
    }

    bptree_node* node = tree->root;

    // 1. 找到第一个叶子节点 (关键思想：不假设 children 一定存在)
    while (!node->is_leaf) {
        node = node->children[0];
    }

    // 2. 沿着 next 打印，直到为 NULL
    printf("叶子链表：");
    while (node) {
        printf("[");
        for (int i = 0; i < node->key_count; i++) {
            printf("%d:%ld", node->keys[i], node->values[i]);
            if (i != node->key_count - 1) {
                printf(" ");
            }
        }
        printf("] -> ");
        node = node->next;
    }

    printf(" NULL");
}

/**
 * 按层打印 B+ 树的具体实现（纯 C 版本）
 */
static void bptree_print_level_bfs(bptree_node* root) {
    if (root == NULL) return;

    // 简单数组队列（B+ 树高度很低，1024 绝对够）
    bptree_node* queue[1024];
    int head = 0;
    int tail = 0;

    // 入队 root
    queue[tail++] = root;

    int level = 0;
    while (head < tail) {
        int level_size = tail - head;
        printf("\nLevel %d:", level);

        for (int i = 0; i < level_size; i++) {
            bptree_node* node = queue[head++];

            // 打印当前节点的 keys
            printf(node->is_leaf ? "<L>[" : "<I>[");
            for (int j = 0; j < node->key_count; j++) {
                printf("%d", node->keys[j]);
                if (j < node->key_count - 1) {
                    printf(", ");
                }
            }
            printf("] ");

            if (node->is_leaf && node->next) {
                printf("-> ");
            }

            // 如果是内部节点，将子节点入队
            if (!node->is_leaf) {
                for (int j = 0; j <= node->key_count; j++) {
                    if (node->children[j] != NULL) {
                        queue[tail++] = node->children[j];
                    }
                }
            }
        }

        printf("\n");
        level++;
    }
}

/**
 * 按层打印 B+ 树的结构
 *  - 区分 内部节点和叶子节点
 *  - 显示 key 的分布
 */
void bptree_print_structure(bptree* tree) {
    if (tree == NULL || tree->root == NULL) {
        printf("B+ 树为空!\n");
        return;
    }

    printf("\n========= B+ 树的结构 =========\n");
    // bptree_print_level_dfs(tree->root, 0);
    bptree_print_level_bfs(tree->root);
    printf("==============================\n");
}

// ===========  销毁 B+ 树  ==========

/**
 * 销毁以 node 为根的子树
 *  *
 * 所有权：
 *  - tree 拥有 tree->root
 *  - internal_node 拥有 children[0...key_count]
 *
 * 方向：父
 *      -> 子1
 *      -> 子2
 *
 * 遍历顺序：后序遍历（先销毁所有子节点，再销毁自己）
 */
void bptree_destroy_node(bptree_node* node) {
    if (node == NULL) return;

    // 1. 如果是内部节点，先递归销毁所有子树
    if (!node->is_leaf) {
        for (int i = 0; i <= node->key_count; i++) {
            if (node->children[i] != NULL) {
                // 按功能理解，逐个释放以 node 的每个孩子为 root 的子树，最后在释放 node 自己
                bptree_destroy_node(node->children[i]);
                node->children[i] = NULL;  // 习惯：销毁后置空
            }
        }
    }

    // 2. 释放节点自带的读写锁（蟹行锁的物质基础）
    pthread_rwlock_destroy(&node->latch);

    // 3. 最后再释放节点内存
    free(node);
}

/**
 * 销毁 B+ 树
 */
void bptree_destroy(bptree* tree) {
    if (tree == NULL) return;

    if (tree->root) {
        bptree_destroy_node(tree->root);
    }

    // 销毁保护 root 指针的入口锁
    pthread_mutex_destroy(&tree->root_lock);

    free(tree);
}

// =============== KV ============
/**
 * 找到最左叶子节点（工具函数）
 *
 */
static bptree_node* bptree_leafmost_leaf(bptree* tree) {
    bptree_node* node = tree->root;
    if (!node) return NULL;

    while (!node->is_leaf) {
        node = node->children[0];
    }

    // node is children

    return node;
}

/**
 * 地毯式搜索
 *  - 日志压缩的核心（Compaction）: 只管在“地表”横向移动
 */
int bptree_scan(bptree* tree, bptree_leaf_visit_fn visit, void* arg) {
    if (!tree || !visit) return -1;  // BPTREE_ERR = -1

    bptree_node* leaf = bptree_leafmost_leaf(tree);
    while (leaf) {
        for (int i = 0; i < leaf->key_count; i++) {
            // 回调调用，每一条数据都会被扔给 visit
            // 如果 visit 返回非0，scan会立刻停止遍历，这给了调用者控制权
            int ret = visit(leaf->keys[i], leaf->values[i], arg);
            if (ret != 0) return 0;  // BPTREE_OK = 0
        }

        leaf = leaf->next;
    }

    return 0;
}

/**
 * 释放路径上所有持有的写锁
 * 清空栈并对其中所有节点执行 unlock
 */
static void bptree_unlock_all_in_path(bptree_write_path* path) {
    if (!path || path->top == 0) return;

    // 建议：从叶子向根反向解锁（Bottom-up Unlock）
    // 理由：虽然写路径是自上而下加锁，但反向解锁可以更早释放底层资源
    // 且能微弱降低高层节点的竞争感
    for (int i = path->top - 1; i >= 0; i--) {
        if (path->nodes[i] != NULL) {
            pthread_rwlock_unlock(&path->nodes[i]->latch);
            // 关键优化：清空指针防止悬空参考
            path->nodes[i] = NULL;
        }
    }
    path->top = 0;
}
