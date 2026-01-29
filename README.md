

# 版本一 非持久化

### 功能
1. 创建并销毁 KVStore 实例
2. 使用 B+ 树储存数据，支持插入、查找、删除操作

### 结构
1. kvstore 通过 bptree 管理数据

### 稳定性
1. 代码可以正常运行，执行基本功能，并且没有报错

# 版本二  最小可用持久化

### KVstore 能把数据写进文件，下次启动时重新插回 B+ 树

### 内存中仍然是 B+ 树，磁盘上只是一个“顺序日志 / 数据文件”

    启动时：
        👉 读文件 → 逐条 insert 到 B+ 树
        
### 文件格式定义
    PUT <key> <value>
    DEL <key>

    PUT 1 100
    PUT 2 200
    PUT 3 300
    从头到尾读文件
    对每一行：
        PUT → bptree_insert
        DEL → bptree_delete
    即 Log Replay（日志重放）


# KVStore —— 基于 B+ 树的持久化 Key-Value 存储引擎

> 一个从零实现的单线程 Key-Value 存储引擎，支持 **B+ 树索引、WAL 日志、崩溃恢复、日志压缩（Compaction）、Snapshot 冷启动优化、只读恢复模式、统一错误码体系**。

------

## 📌 项目简介

**KVStore** 是一个用 C 语言实现的轻量级存储引擎，核心目标是：

- 理解数据库内核中 **索引 + WAL + 崩溃恢复** 的完整链路
- 在不依赖第三方库的前提下，实现一个**可持久化、可恢复、可演进**的 KV 系统
- 强调**系统设计与工程可维护性**，而不仅是功能可跑

该项目采用 **B+ 树作为内存索引结构**，**顺序日志（WAL）作为磁盘持久化格式**，通过 **日志重放（Log Replay）** 与 **Compaction / Snapshot** 机制，保证数据一致性与启动性能。

------

## 核心特性

### 1️⃣ B+ 树内存索引

- 叶子节点形成有序链表，支持顺序扫描
- 插入 / 删除 / 查找复杂度稳定
- Compaction 阶段利用叶子链表进行 **全量有序 dump**

------

### 2️⃣ Write-Ahead Logging（WAL）

- 所有写操作先写日志，再更新内存
- 日志格式（v3）：

```
KVSTORE v3
PUT 1 100|CRC
DEL 2|CRC
```

- 支持 **CRC 校验**，检测日志损坏

------

### 3️⃣ 崩溃恢复（Crash Recovery）

- 启动时进入 **read-only replay 模式**
- 从头顺序扫描日志
- 校验 CRC → 重放有效记录 → 构建 B+ 树
- 发现损坏日志立即停止重放，保证数据一致性

------

### 4️⃣ 日志压缩（Log Compaction）

- 将内存中 **最新状态** dump 为新日志文件
- 使用临时文件 + `rename()` 原子替换
- 显著减少 WAL 体积
- 支持自动触发策略：
  - 日志大小阈值
  - 操作次数阈值

------

### 5️⃣ Snapshot 冷启动优化

- Compaction 后生成 Snapshot
- 启动时优先加载 Snapshot
- 再 replay Snapshot 之后的增量日志
- 大幅降低冷启动时间

------

### 6️⃣ 统一错误码体系（v3.5）

- 所有 public API 只返回 `KVSTORE_ERR_*`
- 内部错误 / IO 错误 / 数据错误严格区分
- 支持 `kvstore_strerror()` 统一错误解释

------

### 7️⃣ 单线程锁 & 只读模式

- 引入全局锁接口（为并发版本预留）
- replay / recovery 阶段强制 `readonly`
- 防止恢复过程中产生脏写

------

## 架构设计

```
┌──────────────────────────────┐
│        Public API            │
│  kvstore_insert/delete/...   │
└─────────────▲────────────────┘
              │
┌─────────────┴────────────────┐
│        Apply Layer           │
│  kvstore_apply_put/del       │
│  - 业务语义                   │
│  - 错误码翻译                 │
└─────────────▲────────────────┘
              │
┌─────────────┴────────────────┐
│   Storage Layer              │
│   - B+ Tree (memory)         │
│   - WAL / Snapshot (disk)    │
└──────────────────────────────┘
```

------

##  项目结构

```text
KVstore/
├── include/
│   ├── kvstore.h
│   └── index/bptree.h
├── src/
│   ├── kvstore.c
│   └── index/bptree.c
├── test/
│   └── test_kvstore.c
├── data.kv
└── README.md
```

------

## 使用示例

```
kvstore* store = kvstore_create("data.kv");

kvstore_insert(store, 1, 100);
kvstore_insert(store, 2, 200);

long value;
kvstore_search(store, 1, &value);

kvstore_delete(store, 2);

kvstore_destroy(store);
```

------

## 已实现版本演进

| 版本 | 特性                          |
| ---- | ----------------------------- |
| v1   | B+ 树内存 KV                  |
| v2   | WAL 持久化 + Log Replay       |
| v3.0 | Log Compaction                |
| v3.1 | 崩溃恢复                      |
| v3.2 | 自动 Compaction               |
| v3.3 | Snapshot                      |
| v3.4 | CRC 校验 + 版本控制           |
| v3.5 | 统一错误码 + read-only replay |

------

##  项目亮点

- 从零实现 **WAL + 崩溃恢复 + Compaction + Snapshot**
- 深度理解 **B+ 树叶子链表在系统中的工程价值**
- 清晰划分 **API / Apply / Storage** 三层结构
- 具备数据库内核级的 **错误码设计与恢复策略**
- 代码强调 **可演进性，而非一次性实现**

------

##  后续规划

- 多线程并发控制（Latch / RWLock）
- MVCC
- Range Scan API
- Binary WAL / mmap
------

