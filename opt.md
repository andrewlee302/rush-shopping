# 功能完善或者优化（待整理）

## （优化）连接池
shopping service使用一个kvstore client发起请求，一个client每次rpc call时都会dial和close。
### 改进
1. 从kvstore端来说，如果每个请求都要重连，显然太费时了，所以kvstore的client应该保持连接，直到用户显式的关闭该连接，或者idle时间太长自动关闭。
2. 从shopping service端来说，即使每个kvstore的client是长连接，但shopping会接受大量用户的并发请求，并发的使用同一个kvstore的client是不安全的。那么可以采用每一个请求使用一个新的client的方式进行读写操作，最后再关闭。更好的方式是采用连接池的方式，从连接池拿到空闲的client，使用完毕后放回连接池，放回连接池的连接并没有关闭，可以通过设置IdleTimeout来关闭那些长时间不使用的连接。可参考Gary Burd的redis，要注意conn.Close并不是关闭连接，而是放回连接池。

## （功能）并发访问kvstore
并发读写kvstore，存在并发访问golang的map数据结构，这不是不安全的操作，golang也会运行时出错禁止该行为。

### 改进
1. 对map的块操作进行原子性加锁
2. 会有无效竞争的开销，如何解决？任务的serilizable？
3. shard？

