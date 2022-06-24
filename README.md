# snowflake_helper
借助etcd自动生成snowflake算法需要的workerId


1. **可以直接使用当前工程的 snowflake算法，比如：**

```go
node, _ := InitSnowFlake(&clientv3.Config{
                        Endpoints:   []string{"127.0.0.1:2379"},
                        DialTimeout: dialTimeout,  
                        }, "testservice")

``` 


2. **或者只用生成的workerID：**

```go
 workerID,err:=GenerateSnowFlakeWorkerID(&clientv3.Config{
                                  Endpoints:   []string{"127.0.0.1:2379"},
                                  DialTimeout: dialTimeout,  
                               }, "testservice")`
```
  
