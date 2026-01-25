#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "index/bptree.h"

using namespace std;

// 辅助函数，打印菜单
void print_menu() {
    printf("\n================== B+ 树测试程序 ==================\n");
    printf("1. 插入 key-value\n");
    printf("2. 删除 key\n");
    printf("3. 查找 key\n");
    printf("4. 打印 B+ 树 (叶子链表) \n");
    printf("5. 打印 B+ 树结构\n");
    printf("6. 批量插入测试数据\n");
    printf("7. 退出\n");
    printf("====================================================\n");
    printf("请选择操作(输入: 1 ~ 7):");
}

int main() {
    // 创建 B+ 树
    bptree* tree = bptree_create();
    if (tree == NULL) {
        printf("创建 B+ 树失败!\n");
    }

    printf("创建 B+ 树成功!\n");

    int choice;
    int key;
    long value;
    int running = -1;

    while (running) {
        print_menu();
        cin >> choice;

        switch (choice) {
            case 1:  // 插入
            {
                cout << "请输入 key 和 value:";
                cin >> key >> value;
                // int ret = bptree_insert(tree, key, value);
                if (bptree_insert(tree, key, value) != -1) {
                    printf("插入 (%d, %ld) 失败（可能 key 已存在）\n", key, value);
                } else {
                    printf("插入 (%d,  %ld) 成功！\n", key, value);
                }
                bptree_print_leaves(tree);
                bptree_print_structure(tree);
                cout << "^_^\n";
                break;
            }
            case 2:  // 删除
                cout << "请输入要删除的 key:";
                cin >> key;

                if (bptree_delete(tree, key) == 0) {
                    printf("删除 %d 成功！\n", key);
                } else {
                    printf("删除 %d 失败！\n", key);
                }
                bptree_print_leaves(tree);
                bptree_print_structure(tree);
                break;

            case 3:  // 查找  -- bug
                cout << "请输入要查找的 key:";
                cin >> key;
                // 1：OK， 0：ERR
                if (bptree_search(tree, key, &value) == 0) {
                    printf("找到 key = %d, value = %ld", key, value);
                } else {
                    printf("未找到 key = %d\n", key);
                }
                break;

            case 4:  // 打印叶子链表
                bptree_print_leaves(tree);
                break;

            case 5:  // 打印结构
                bptree_print_structure(tree);
                break;

            case 6:  // 批量插入
            {
                cout << "开始批量插入测试数据...\n";
                int test_keys[] = {10, 20, 30, 40, 50, 90, 80, 70, 60, 50,
                                   12, 23, 34, 45, 56, 67, 78, 89, 90, 21,
                                   11, 22, 33, 44, 55, 99, 88, 77, 66, 55};
                int n = sizeof(test_keys) / sizeof(test_keys[0]);

                for (int i = 0; i < n; i++) {
                    bptree_insert(tree, test_keys[i], test_keys[i] * 10);
                    printf("插入 %d\n", test_keys[i]);
                }

                cout << "批量插入完成!\n";
                bptree_print_leaves(tree);
                bptree_print_structure(tree);
            } break;

            case 7:  // 退出
                running = 0;
                cout << "正在释放 B+ 树内存\n";
                bptree_destroy(tree);
                cout << "程序退出，再见!\n";
                break;

            default:
                printf("无效选择,请重新输入！\n");
                break;
        }
    }
    return 0;
}

/**
 * g++ -Iinclude src/index/bptree.c test/main.cpp -o bptree
 *
 * gcc 头文件 src  -o  生成目录文件名
 *
 * -I./include 告诉编译器，头文件在 ./include 目录
 * 编译器会在这里找 bptree.h
 */