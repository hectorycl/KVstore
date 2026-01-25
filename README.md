

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