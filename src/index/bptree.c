// KVstore/src/index/bptree.c

#include "index/bptree.h"

#include <stdio.h>
#include <stdlib.h>
// #include <iostream>

/**
 * bptree_internal.h 内容，整体挪到这里顶部：
 *  bptree_node 世界上只有 bptree.c 知道
 *  main / test / kvstore 永远碰不到它
 *
 *  */

// ================== 宏定义（唯一来源） ==================
#define ORDER 6
#define MAX_KEYS (ORDER - 1)
#define MAX_CHILDREN ORDER
#define MIN_KEYS ((MAX_KEYS + 1) / 2)

// #define BPTREE_OK 0
// #define BPTREE_ERR -1

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
} bptree_node;

struct _bptree {
    bptree_node* root;
};

// ================== Internal API ==================
// ========== 插入（内部实现） ==========
static bptree_node* bptree_create_node(int is_leaf);
static int bptree_contains(bptree* tree, int key);
static void bptree_insert_into_leaf(bptree_node* leaf, int key, long value);
static int bptree_split_leaf_and_insert(bptree* tree, bptree_node* leaf, int key, long value);
static int bptree_insert_into_parent(bptree* tree, bptree_node* parent, int insert_index,
                                     int key, bptree_node* right_child);
static int bptree_split_internal_and_insert(bptree* tree, bptree_node* node,
                                            int insert_index, int key, bptree_node* right_child);

// ========== 删除（内部实现） ==========
static int bptree_delete_from_leaf(bptree* tree, bptree_node* leaf, int key);
static int bptree_is_underflow(bptree* tree, bptree_node* node);
static bptree_node* bptree_get_left_sibling(bptree_node* node);
static bptree_node* bptree_get_right_sibling(bptree_node* node);
static int bptree_borrow_from_left_leaf(bptree_node* leaf);
static int bptree_borrow_from_right_leaf(bptree_node* leaf);
static int bptree_borrow_from_left_internal(bptree_node* node);
static int bptree_borrow_from_right_internal(bptree_node* node);
static int bptree_merge_leaf(bptree* tree, bptree_node* left, int* out_parent_key_idx);
static int bptree_merge_internal(bptree* tree, bptree_node* left, int* out_parent_key_idx);
static int bptree_delete_from_leaf(bptree* tree, bptree_node* leaf, int key);
static int bptree_delete_from_internal(bptree* tree, bptree_node* node, int key_index);
static void bptree_delete_fixup(bptree* tree, bptree_node* node);
static void bptree_fix_leaf(bptree* tree, bptree_node* leaf);
static void bptree_fix_internal(bptree* tree, bptree_node* node);

// ========== 查找（内部） ==========
static bptree_node* bptree_find_leaf(bptree* tree, int key);

// ========== 打印 / 调试 ==========
static void bptree_print_level_bfs(bptree_node* root);

// ========== 销毁（内部） ==========
static void bptree_destroy_node(bptree_node* node);

// =========== KV 相关操作  ===========
static bptree_node* bptree_leafmost_leaf(bptree* tree);
int bptree_scan(bptree* tree, bptree_leaf_visit_fn visit, void* arg);

// 创建一个新的 B+ 树节点
bptree_node* bptree_create_node(int is_leaf) {
    bptree_node* node = (bptree_node*)malloc(sizeof(bptree_node));
    if (!node) {
        perror("malloc bptree_node");
        exit(EXIT_FAILURE);
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

    return node;
}

// 创建一个新的 B+ 树
bptree* bptree_create() {
    bptree* tree = (bptree*)malloc(sizeof(bptree));  // 分配 B+ 树结构体内存
    if (tree == NULL) {
        fprintf(stderr, "内存分配失败！\n");
        exit(EXIT_FAILURE);
    }
    tree->root = bptree_create_node(1);  // 根节点初始一定是叶子
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

    // 树刚创建时，root 是叶子节点，并且 key_count == 0 - 合法！
    //  if (current == NULL || current->key_count == 0) {
    //      return NULL;
    //  }

    if (current->is_leaf == 0) return NULL;

    // printf("\n[DEBUG] find_leaf is_leaf=%d key_count=%d",
    //        current->is_leaf, current->key_count);

    return current;
}

// 在 B+ 树中搜索指定 key 的值,如果找到则将值存入 out_value 并返回 1，否则返回 0
int bptree_search(bptree* tree, int key, long* out_value) {
    if (tree == NULL || tree->root == NULL)
        return BPTREE_ERR;
    if (out_value == NULL)
        return BPTREE_ERR;

    bptree_node* leaf = bptree_find_leaf(tree, key);
    if (leaf == NULL) {
        return BPTREE_ERR;
    }

    for (int i = 0; i < leaf->key_count; i++) {
        if (leaf->keys[i] == key) {
            *out_value = leaf->values[i];
            return BPTREE_OK;
        } else if (leaf->keys[i] > key) {
            break;
        }
    }
    return BPTREE_ERR;
}

/**
 * 是否包含 key, 复用代码，不碰 value
 */
int bptree_contains(bptree* tree, int key) {
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
    // 3. 插入 key / value
    leaf->keys[idx] = key;
    leaf->values[idx] = value;
    leaf->key_count++;

    // printf("[DEBUG] after insert, key_count = %d\n", leaf->key_count);
}

// 插入键值对
int bptree_insert(bptree* tree, int key, long value) {
    if (tree == NULL || tree->root == NULL)
        return BPTREE_ERR;

    bptree_node* leaf = bptree_find_leaf(tree, key);
    if (leaf == NULL)
        return BPTREE_ERR;

    // 唯一一次重复 key 判断
    int flag = bptree_contains(tree, key);
    if (flag == BPTREE_OK) {
        // fprintf(stderr, "键 %d 已存在，插入失败！\n", key);
        // return BPTREE_ERR;
        ;
    }

    // 1. 查找 key 是否已经在叶子节点中
    for (int i = 0; i < leaf->key_count; i++) {
        if (leaf->keys[i] == key) {
            // --- 关键修改：如果找到了，直接更新 value 并返回 ---
            leaf->values[i] = value;
            //printf("键 %d 已存在，已更新值为 %ld\n", key, value);
            return BPTREE_OK;
        }
    }

    // 2. 如果没找到，再走原来的插入/分裂逻辑
    // 叶子节点有空间，直接插入
    if (leaf->key_count < MAX_KEYS) {
        bptree_insert_into_leaf(leaf, key, value);
        return BPTREE_OK;
    } else {  // 叶子节点已满，需分裂
        return bptree_split_leaf_and_insert(tree, leaf, key, value);
    }
}

/**
 * 分裂叶子节点并插入新的键值对
 * 目标：
 *  1. 将（leaf + 新 key）共 MAX_KEYS + 1 个
 *  2. 左节点保留前 half 个 key
 *  3. 右节点保留后半个 key
 *  4. 将右节点的第一个 key 上推到父节点
 */
int bptree_split_leaf_and_insert(bptree* tree, bptree_node* leaf, int key, long value) {
    // 1. 创建新叶子节点 is_leaf = 1
    bptree_node* right = bptree_create_node(1);
    right->parent = leaf->parent;

    // 2. 临时数组，保存 MAX_KEYS 个 key / value
    int* temp_keys = (int*)malloc(sizeof(int) * (leaf->key_count + 1));
    long* temp_values = (long*)malloc(sizeof(long) * (leaf->key_count + 1));
    if (temp_keys == NULL || temp_values == NULL) return BPTREE_ERR;

    // 3. 将原 leaf + 新 key 拷贝进临时数组（有序）
    int idx = 0;
    while (idx < leaf->key_count && leaf->keys[idx] < key) {
        idx++;
    }

    for (int i = 0; i < idx; i++) {
        temp_keys[i] = leaf->keys[i];
        temp_values[i] = leaf->values[i];
    }
    temp_keys[idx] = key;
    temp_values[idx] = value;
    for (int i = leaf->key_count; i > idx; i--) {
        temp_keys[i] = leaf->keys[i - 1];
        temp_values[i] = leaf->values[i - 1];
    }

    // 4. 重新分配 leaf / right 的 key
    int total = leaf->key_count + 1;
    int split = (total + 1) / 2;
    for (int i = 0; i < split; i++) {
        leaf->keys[i] = temp_keys[i];
        leaf->values[i] = temp_values[i];
    }
    for (int i = split, j = 0; i < total; i++, j++) {
        right->keys[j] = temp_keys[i];
        right->values[j] = temp_values[i];
    }
    leaf->key_count = split;
    right->key_count = total - split;
    free(temp_keys);
    free(temp_values);

    // 5. 维护 leaf 链表指针
    right->next = leaf->next;
    leaf->next = right;

    // 6. 将分裂 key 插入父节点（或者新建根） todo
    if (leaf->parent != NULL) {
        bptree_node* parent = leaf->parent;

        int insert_index = bptree_find_child_index(parent, leaf);
        if (insert_index < 0) {
            return BPTREE_ERR;
        }

        if (bptree_insert_into_parent(tree, parent, insert_index, right->keys[0], right) != BPTREE_OK) {
            return BPTREE_ERR;
        }
    } else {
        // todo 创建新根
        bptree_node* root = bptree_create_node(0);
        root->keys[0] = right->keys[0];
        root->children[0] = leaf;
        root->children[1] = right;
        root->parent = NULL;
        root->key_count = 1;

        leaf->parent = root;
        right->parent = root;
        tree->root = root;
    }

    // printf("split leaf: promote key = %d\n", right->keys[0]);

    return BPTREE_OK;
}

/**
 * 父节点插入函数(父节点一定不为 NULL)
 * 1. 取出 node 的父节点 parent
 * 2. parent 以及右侧的叔父节点右移动一位，腾出位置
 * 3. right 的 keys[0] 上移、插入 right 到 parent
 * 4. 更新 key_count
 *
 *
 */
int bptree_insert_into_parent(bptree* tree, bptree_node* parent, int insert_index,
                              int key, bptree_node* right_child) {
    // 1. 有空间，直接插
    if (parent->key_count < MAX_KEYS) {
        for (int i = parent->key_count; i > insert_index; i--) {
            parent->keys[i] = parent->keys[i - 1];
        }
        for (int i = parent->key_count + 1; i > insert_index + 1; i--) {
            parent->children[i] = parent->children[i - 1];
        }

        parent->keys[insert_index] = key;
        parent->children[insert_index + 1] = right_child;
        right_child->parent = parent;
        parent->key_count++;
        return BPTREE_OK;
    }

    // 2. 满了，走 split
    return bptree_split_internal_and_insert(tree, parent, insert_index, key, right_child);
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
 */
int bptree_split_internal_and_insert(bptree* tree, bptree_node* node,
                                     int insert_index, int key, bptree_node* right_child) {
    // todo
    // 1. 临时数组：keys + children
    int total_keys = node->key_count + 1;

    int temp_keys[MAX_KEYS + 1];
    bptree_node* temp_children[MAX_KEYS + 2];

    // 2. 拷贝 children（先）
    for (int i = 0; i <= node->key_count; i++) {
        temp_children[i] = node->children[i];
    }
    for (int i = 0; i < node->key_count; i++) {
        temp_keys[i] = node->keys[i];
    }

    // 3. 插入新的 key / child
    for (int i = node->key_count; i > insert_index; i--) {
        temp_keys[i] = temp_keys[i - 1];
    }
    temp_keys[insert_index] = key;

    for (int i = node->key_count + 1; i > insert_index + 1; i--) {
        temp_children[i] = temp_children[i - 1];
    }
    temp_children[insert_index + 1] = right_child;
    right_child->parent = node;

    // 4. 分裂
    int mid = total_keys / 2;
    int middle_key = temp_keys[mid];

    // 5. 创建 right internal
    bptree_node* right = bptree_create_node(0);
    right->parent = node->parent;

    // 6. 左边保留
    node->key_count = mid;
    for (int i = 0; i < mid; i++) {
        node->keys[i] = temp_keys[i];
        node->children[i] = temp_children[i];
    }
    node->children[mid] = temp_children[mid];

    // 7. 右边拷贝
    int r = 0;
    for (int i = mid + 1; i < total_keys; i++, r++) {
        right->keys[r] = temp_keys[i];
        right->children[r] = temp_children[i];
        if (right->children[r]) {
            right->children[r]->parent = right;
        }
    }
    right->children[r] = temp_children[total_keys];
    if (right->children[r]) {
        right->children[r]->parent = right;
    }
    right->key_count = total_keys - mid - 1;

    // 8. 处理父节点
    if (node->parent == NULL) {
        // 新建根
        bptree_node* root = bptree_create_node(0);
        root->keys[0] = middle_key;
        root->children[0] = node;
        root->children[1] = right;
        root->key_count = 1;

        node->parent = root;
        right->parent = root;
        tree->root = root;
    } else {
        int parent_idx = bptree_find_child_index(node->parent, node);
        if (bptree_insert_into_parent(tree, node->parent,
                                      parent_idx, middle_key, right) != BPTREE_OK) {
            return BPTREE_ERR;
        }
    }

    return BPTREE_OK;
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
bptree_node* bptree_get_left_sibling(bptree_node* node) {
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
bptree_node* bptree_get_right_sibling(bptree_node* node) {
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
 */
int bptree_borrow_from_left_leaf(bptree_node* leaf) {
    bptree_node* left = bptree_get_left_sibling(leaf);
    if (left == NULL) {
        return BPTREE_ERR;
    }

    int cnt = left->key_count;
    if (cnt <= MIN_KEYS) {
        return BPTREE_ERR;
    }
    // 能借
    int idx = bptree_find_child_index(leaf->parent, left);

    // 腾位置 + 下移 key + 上移 key
    for (int i = leaf->key_count; i > 0; i--) {
        leaf->keys[i] = leaf->keys[i - 1];
        leaf->values[i] = leaf->values[i - 1];
    }
    leaf->keys[0] = left->keys[left->key_count - 1];
    leaf->values[0] = left->values[left->key_count - 1];
    leaf->parent->keys[idx] = leaf->keys[0];

    // 清理脏数据
    left->keys[left->key_count - 1] = 0;
    left->values[left->key_count - 1] = 0;

    // 更新 count
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
int bptree_borrow_from_right_leaf(bptree_node* leaf) {
    bptree_node* right = bptree_get_right_sibling(leaf);
    if (right == NULL) {
        return BPTREE_ERR;
    }

    int cnt = right->key_count;
    if (cnt <= MIN_KEYS) {
        return BPTREE_ERR;
    }

    int idx = bptree_find_child_index(leaf->parent, leaf);
    if (idx >= leaf->parent->key_count) {
        return BPTREE_ERR;
    }

    // 借最小 key
    leaf->keys[leaf->key_count] = right->keys[0];
    leaf->values[leaf->key_count] = right->values[0];

    for (int i = 0; i < right->key_count - 1; i++) {
        right->keys[i] = right->keys[i + 1];
        right->values[i] = right->values[i + 1];
    }

    // 更新父 key（在 key_count-- 之前）
    leaf->parent->keys[idx] = right->keys[0];

    // 清理脏数据
    right->keys[right->key_count - 1] = 0;
    right->values[right->key_count - 1] = 0;

    right->key_count--;
    leaf->key_count++;

    return BPTREE_OK;
}

/**
 * 从左兄弟借一个 key + child (内部节点)
 *
 * 使用前提：
 *  - 保证 node 需要借(node 一定是 underflow): node->key_count < MAX_KEYS
 *
 * 旋转规则：
 *  - parent 的分隔 key 下移到 node (保证 node 需要借)
 *  - left 的最大 key 上移到 parent
 *  - left 的最右 child 移动到 node 的最左侧
 *
 * @return
 *  - BPTREE_OK   借成功，结构已修复
 *  - BPTREER_ERR 借失败（左兄弟不存在或者 key 不足）
 */
int bptree_borrow_from_left_internal(bptree_node* node) {
    if (node == NULL || node->parent == NULL) return BPTREE_ERR;

    int idx = bptree_find_child_index(node->parent, node);
    if (idx <= 0 || idx == -1) return BPTREE_ERR;

    bptree_node* left = node->parent->children[idx - 1];
    if (left == NULL || left->key_count <= MIN_KEYS) return BPTREE_ERR;

    // 前提保证: node->key_count < MAX_KEYS
    if (node->key_count >= MAX_KEYS) return BPTREE_ERR;

    // 腾位置
    for (int i = node->key_count; i > 0; i--) {
        node->keys[i] = node->keys[i - 1];
    }
    for (int i = node->key_count; i >= 0; i--) {
        node->children[i + 1] = node->children[i];
    }

    // 不能断言，防止段错误
    if (left->children[left->key_count] == NULL) return BPTREE_ERR;

    // 借 key 和 child
    node->keys[0] = node->parent->keys[idx - 1];
    node->children[0] = left->children[left->key_count];
    node->parent->keys[idx - 1] = left->keys[left->key_count - 1];  // parent->keys[idx - 1] 是 left | node 的分隔 key -todo

    // 结构维护
    node->children[0]->parent = node;

    // 清理脏数据 left
    left->keys[left->key_count - 1] = 0;
    left->children[left->key_count] = NULL;

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
int bptree_borrow_from_right_internal(bptree_node* node) {
    if (!node || !node->parent) return BPTREE_ERR;

    // idx 是 child 的下标
    int idx = bptree_find_child_index(node->parent, node);
    if (idx < 0 || idx >= node->parent->key_count) return BPTREE_ERR;

    bptree_node* right = node->parent->children[idx + 1];
    if (right == NULL || right->key_count <= MIN_KEYS) return BPTREE_ERR;

    if (node->key_count >= MAX_KEYS) return BPTREE_ERR;

    if (right->children[0] == NULL) return BPTREE_ERR;
    // 能借
    node->keys[node->key_count] = node->parent->keys[idx];
    node->children[node->key_count + 1] = right->children[0];

    node->parent->keys[idx] = right->keys[0];

    // 左移 key & children
    for (int i = 0; i < right->key_count - 1; i++) {
        right->keys[i] = right->keys[i + 1];
    }
    for (int i = 0; i < right->key_count; i++) {
        right->children[i] = right->children[i + 1];
    }

    // 结构维护
    if (node->children[node->key_count + 1] == NULL) return BPTREE_ERR;
    node->children[node->key_count + 1]->parent = node;

    // 清洗脏数据
    right->keys[right->key_count - 1] = 0;
    right->children[right->key_count] = NULL;

    node->key_count++;
    right->key_count--;

    return BPTREE_OK;
}

// 合并（真正的递归删除）
/**
 * 合并 left 和它紧邻的右兄弟
 * 并从父节点删除对应 key
 *
 * 说明：
 *  - 两个节点合并，一左一右，传进来的为 left
 *  - 合并后的的节点为 left
 *
 * @return
 *  - BPTREE_OK : 合成成功
 *  - BPTREE_ERR: 合并失败
 */

int bptree_merge_leaf(bptree* tree, bptree_node* left, int* out_parent_key_idx) {
    if (left == NULL || left->parent == NULL) return BPTREE_ERR;
    if (out_parent_key_idx == NULL) return BPTREE_ERR;

    bptree_node* parent = left->parent;
    int idx = bptree_find_child_index(parent, left);

    // 必须有右兄弟
    if (idx < 0 || idx >= parent->key_count) {
        return BPTREE_ERR;
    }

    // todo
    bptree_node* right = parent->children[idx + 1];
    if (right == NULL) {
        return BPTREE_ERR;
    }

    // 合并前必须满足容量
    if (left->key_count + right->key_count > MAX_KEYS) {
        return BPTREE_ERR;
    }

    // 1.拷贝数据
    int old = left->key_count;
    for (int j = 0; j < right->key_count; j++) {
        left->keys[old + j] = right->keys[j];
        left->values[old + j] = right->values[j];
    }

    // 2. 维护链表
    left->next = right->next;

    // 3. 更新计数
    left->key_count += right->key_count;

    // 4. 返回父节点要删除的 key 的位置
    if (out_parent_key_idx == NULL)
        return BPTREE_ERR;
    *out_parent_key_idx = idx;

    // 不懂 parent , 不 free right (由外部统一释放)

    // // 移动 parent 的 key & children
    // for (int i = child_idx - 1; i < parent->key_count - 1; i++) {
    //     parent->keys[i] = parent->keys[i + 1];
    // }
    // for (int i = child_idx + 1; i <= parent->key_count; i++) {
    //     parent->children[i] = parent->children[i + 1];
    // }

    // parent->keys[parent->key_count - 1] = 0;
    // parent->children[parent->key_count] = NULL;
    // parent->key_count--;

    // // 5. 释放 right
    // free(right);
    return BPTREE_OK;
}

/**
 * 合并内部节点, 不会用于 root
 *
 * 规则：
 *  - parent 的 key 下沉
 *  - right 的 key + child 接到 left 后面
 *  - 更新 child->parent
 *  - parent 中删除分隔的 key & right child
 *  - free(right)
 */
int bptree_merge_internal(bptree* tree, bptree_node* left, int* out_parent_key_idx) {
    if (left == NULL || left->parent == NULL) return BPTREE_ERR;

    bptree_node* parent = left->parent;

    // todo -1 ?
    int child_idx = bptree_find_child_index(parent, left);
    if (child_idx < 0 || child_idx >= parent->key_count) return BPTREE_ERR;

    bptree_node* right = parent->children[child_idx + 1];
    if (right == NULL) return BPTREE_ERR;

    if (left->key_count + right->key_count + 1 > MAX_KEYS) {
        return BPTREE_ERR;
    }

    // 1. 把 parent 的分隔 key 移到 left
    int old_count = left->key_count;
    left->keys[old_count] = parent->keys[child_idx];

    // 2. 把 right 的 keys 和 children 合并到 left
    for (int j = 0; j < right->key_count; j++) {
        left->keys[old_count + 1 + j] = right->keys[j];
    }
    for (int j = 0; j <= right->key_count; j++) {
        // assert (条件) -> 如果条件为真：什么也不发生，继续向下执行。
        // 否则程序终止，打印错误信息 - todo

        if (right->children[j] == NULL) return BPTREE_ERR;
        left->children[old_count + 1 + j] = right->children[j];
        left->children[old_count + 1 + j]->parent = left;
    }

    left->key_count += right->key_count + 1;

    // 3. 返回 parent 中要删除的 key
    if (out_parent_key_idx == NULL) return BPTREE_ERR;
    *out_parent_key_idx = child_idx;

    // 在 merge_internal 里，提前做了“父节点结构删除”  ( 错误 ❌， 破坏了指责一致性！！！)
    // 4. 移动 parent 的 key & child
    // todo
    // for (int i = child_idx; i < parent->key_count - 1; i++) {
    //     parent->keys[i] = parent->keys[i + 1];
    // }

    // for (int i = child_idx; i <= parent->key_count; i++) {
    //     parent->children[i] = parent->children[i + 1];
    // }

    // parent->keys[parent->key_count - 1] = 0;
    // parent->children[parent->key_count] = NULL;
    // parent->key_count--;

    free(right);
    return BPTREE_OK;
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

    // 找到位置
    int idx;
    for (idx = 0; idx < leaf->key_count; idx++) {
        if (leaf->keys[idx] == key) {
            break;
        }
    }

    // 若没找到
    if (idx == leaf->key_count) {
        printf("key %d 不存在！\n", key);
        return BPTREE_ERR;
    }

    // 覆盖
    for (int i = idx; i < leaf->key_count - 1; i++) {
        leaf->keys[i] = leaf->keys[i + 1];
        leaf->values[i] = leaf->values[i + 1];
    }
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
 */

int bptree_delete_from_internal(bptree* tree, bptree_node* node, int key_index) {
    if (node == NULL || key_index < 0 || key_index >= node->key_count) {
        return BPTREE_ERR;
    }

    int old_key_count = node->key_count;

    // 1. 删除 key
    for (int i = key_index; i < old_key_count - 1; i++) {
        node->keys[i] = node->keys[i + 1];
    }

    // 2. 删除 child
    int child_index = key_index + 1;
    for (int i = child_index; i <= old_key_count; i++) {
        node->children[i] = node->children[i + 1];
    }

    node->key_count--;
    node->keys[node->key_count] = 0;
    node->children[node->key_count + 1] = NULL;

    // 特判 root 缩高(todo)
    if (node == tree->root && tree->root->key_count == 0) {
        bptree_node* old_root = tree->root;
        // old_root 可能为叶子节点
        if (!old_root->is_leaf) {
            tree->root = old_root->children[0];
            if (tree->root) tree->root->parent = NULL;
        } else {
            tree->root = NULL;
        }
        free(old_root);
        return BPTREE_OK;
    }

    // 下溢处理
    if (node->key_count < MIN_KEYS) {
        // borrow
        if (bptree_borrow_from_left_internal(node) == BPTREE_OK ||
            bptree_borrow_from_right_internal(node) == BPTREE_OK) {
            return BPTREE_OK;
        }

        // root 不再向上 merge
        if (node->parent == NULL) {
            return BPTREE_OK;
        }

        // merge 内部节点
        int parent_key_idx;
        if (bptree_merge_internal(tree, node, &parent_key_idx) != BPTREE_OK) {
            return BPTREE_ERR;
        }

        // 递归删除 parent 的 key
        return bptree_delete_from_internal(tree, node->parent, parent_key_idx);
    }

    return BPTREE_OK;
}

/**
 * 修复 leaf 结构
 *
 * 步骤：
 *  - 1. 借左/右
 *  - 2. 无法借时，merge
 *  - 3. merge 后递归修复父节点
 */
void bptree_fix_leaf(bptree* tree, bptree_node* leaf) {
    // 1. borrow left
    if (bptree_borrow_from_left_leaf(leaf) == BPTREE_OK) return;

    // 2. borrow right
    if (bptree_borrow_from_right_leaf(leaf) == BPTREE_OK) return;

    // 3. merge
    int parent_key_idx = -1;
    bptree_node* parent = leaf->parent;

    bptree_node* left = bptree_get_left_sibling(leaf);
    bptree_node* right = bptree_get_right_sibling(leaf);

    bptree_node* merge_left = NULL;
    bptree_node* merge_right = NULL;

    if (left) {
        // left + leaf
        merge_left = left;
        merge_right = leaf;
    } else if (right) {
        // leaf + right
        merge_left = leaf;
        merge_right = right;
    } else {
        // 非 root 不可能发生
        return;
    }

    if (bptree_merge_leaf(tree, merge_left, &parent_key_idx) != BPTREE_OK) {
        return;
    }

    // 删除 parent 中的 key / child
    bptree_delete_from_internal(tree, parent, parent_key_idx);

    // 释放被吞掉的叶子
    free(merge_right);

    // 修复 parent
    if (parent && bptree_is_underflow(tree, parent)) {
        bptree_fix_internal(tree, parent);
    }
}

/**
 * 修复 internal node 结构
 *
 * 步骤：
 *  - 1. 借左/右
 *  - 2. 借不到，merge
 *  - 3. merge 后修复
 */
void bptree_fix_internal(bptree* tree, bptree_node* node) {
    // 1. borrow
    if (bptree_borrow_from_left_internal(node) == BPTREE_OK) return;
    if (bptree_borrow_from_right_internal(node) == BPTREE_OK) return;

    bptree_node* parent = node->parent;
    if (parent == NULL) return;

    int parent_key_idx = -1;

    bptree_node* left = bptree_get_left_sibling(node);
    bptree_node* right = bptree_get_right_sibling(node);

    bptree_node* merge_left = NULL;
    bptree_node* merge_right = NULL;

    if (left) {
        // left + node
        merge_left = left;
        merge_right = node;
    } else if (right) {
        // node + right
        merge_left = node;
        merge_right = right;
    } else {
        // 非 root 不可能
        return;
    }

    printf("[DEBUG] parent key_count before delete = %d\n", parent->key_count);
    if (bptree_merge_internal(tree, merge_left, &parent_key_idx) != BPTREE_OK) {
        return;
    }

    // ⭐ 删除 parent 中的 key / child
    bptree_delete_from_internal(tree, parent, parent_key_idx);

    // ⭐ 释放被吞的 internal
    free(merge_right);

    // ⭐ root 特判
    if (parent == tree->root && parent->key_count == 0) {
        printf("\n[DEBUG] root shrink, promote child %p", parent->children[0]);
        tree->root = merge_left;
        merge_left->parent = NULL;
        free(parent);
        return;
    }

    // ⭐ 递归修复 parent
    if (bptree_is_underflow(tree, parent)) {
        bptree_fix_internal(tree, parent);
    }
}

/**
 * 删除后修复树结构（borrow / merge / 递归）
 */
void bptree_delete_fixup(bptree* tree, bptree_node* node) {
    // leaf
    if (node->is_leaf) {
        bptree_fix_leaf(tree, node);
    } else {  // internal
        bptree_fix_internal(tree, node);
    }
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
    bptree_node* leaf = bptree_find_leaf(tree, key);

    if (leaf == NULL) {
        return BPTREE_ERR;
    }

    // 1. 叶子删除
    if (bptree_delete_from_leaf(tree, leaf, key) == BPTREE_ERR) {
        return BPTREE_ERR;
    }

    // int fg = bptree_is_underflow(tree, leaf);
    // if (fg == BPTREE_OK)
    //     printf("下溢\n");
    // else
    //     printf("没有下溢\n");

    // 2. 如果叶子下溢，修复
    if (leaf != tree->root && bptree_is_underflow(tree, leaf)) {
        bptree_delete_fixup(tree, leaf);
    }

    // 3. 如果 root 变空，更新 root
    if (tree->root->key_count == 0 && !tree->root->is_leaf) {
        bptree_node* new_root = tree->root->children[0];
        free(tree->root);

        tree->root = new_root;
        if (new_root) new_root->parent = NULL;
    }

    return BPTREE_OK;

    // // 2. 未下溢，结束
    // if (!bptree_is_underflow(tree, leaf)) {
    //     return BPTREE_OK;
    // }

    // // root 叶子，无需 merge / 向上递归
    // if (leaf->parent == NULL) {
    //     return BPTREE_OK;
    // }

    // // 3. boorow
    // if (bptree_borrow_from_left_leaf(leaf) == BPTREE_OK) {
    //     return BPTREE_OK;
    // }
    // if (bptree_borrow_from_right_leaf(leaf) == BPTREE_OK) {
    //     return BPTREE_OK;
    // }

    // // 4. merge
    // int parent_key_index = -1;
    // bptree_node* left = bptree_get_left_sibling(leaf);
    // bptree_node* merge_node = (left != NULL) ? left : leaf;

    // if (bptree_merge_leaf(tree, merge_node, &parent_key_index) != BPTREE_OK) {
    //     return BPTREE_ERR;
    // }

    // if (parent_key_index < 0) {
    //     return BPTREE_ERR;
    // }

    // // 5. 向上递归
    // return bptree_delete_from_internal(tree, merge_node->parent, parent_key_index);
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

    if (node->is_leaf) {
        free(node);
    } else {  // 内部节点  -- todo
        for (int i = 0; i <= node->key_count; i++) {
            if (node->children[i] != NULL) {
                // 按功能理解，逐个释放以 node 的每个孩子为 root 的子树，最后在释放 node 自己
                bptree_destroy_node(node->children[i]);
            }
        }
        free(node);
    }
}

/**
 * 销毁 B+ 树
 */
void bptree_destroy(bptree* tree) {
    if (tree == NULL) return;

    bptree_destroy_node(tree->root);
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
