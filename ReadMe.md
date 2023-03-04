## Resp协议
RESP 定义了 5 种格式：

简单字符串（Simple String）： 服务器用来返回简单的结果，比如 "OK" 非二进制安全，且不允许换行
错误信息（Error）：服务器用来返回简单的错误信息，比如 "ERR Invalid Synatx" 非二进制安全，且不允许换行
整数（Integer）：llen、scard 等命令的返回值，64 位有符号整数
字符串（Bulk String）：二进制安全字符串，比如 get 等命令的返回值
数组（Array，又称 Multi Bulk Strings）：Bulk String 数组，客户端发送指令以及 lrange 等命令响应的格式

RESP 通过第一个字符来表示格式：

简单字符串：以"+" 开始， 如："+OK\r\n"
错误：以"-" 开始，如："-ERR Invalid Synatx\r\n"
整数：以":"开始，如：":1\r\n"
字符串：以 $ 开始
数组：以 * 开始

## 内存数据库
与单线程的 Redis 不同我们实现的 Redis（godis）是并行工作的，所以我们必须考虑各种并发安全问题。常见的并发安全哈希表设计有几种：

sync.map：
Golang 官方提供的并发哈希表，适合读多写少的场景。但是在 m.dirty 刚被提升后会将 m.read 复制到新的 m.dirty 中，在数据量较大的情况下复制操作会阻塞所有协程，存在较大的隐患。

juc.ConcurrentHashMap：
Java 的并发哈希表采用分段锁实现。在进行扩容时访问哈希表线程都将协助进行 rehash 操作，在 rehash 结束前所有的读写操作都会阻塞。因为缓存数据库中键值对数量巨大且对读写操作响应时间要求较高，使用 juc 的策略是不合适的。

memcached hashtable：
在后台线程进行 rehash 操作时，主线程会判断要访问的哈希槽是否已被 rehash 从而决定操作 old_hashtable 还是操作 new_hashtable。这种设计被称为渐进式 rehash 它的优点是 rehash 操作基本不会阻塞主线程的读写，是最理想的的方案。

但渐进式 rehash 的实现非常复杂，所以 godis 采用 Golang 社区广泛使用的分段锁策略（非上面的三种），就是将 key 分散到固定数量的 shard 中避免进行整体 rehash 操作。shard 是有锁保护的 map，当 shard 进行 rehash 时会阻塞 shard 内的读写，但不会对其他 shard 造成影响。
![img.png](img.png)

优化TODO
1. 优雅关闭
2. 更多的指令集

