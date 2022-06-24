package snowflakehelper

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
	"testing"
)

func TestInitSnowFlake(t *testing.T) {

	//测试多个进程同时启动时，获得的workerId不允许重复

	idMap := make(map[int64]string)

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			node, _ := InitSnowFlake(&clientv3.Config{
				Endpoints:   []string{"127.0.0.1:2379"},
				DialTimeout: dialTimeout,
			}, "testservice")

			if _, ok := idMap[node.WorkerID()]; ok {
				t.Errorf("duplicated id:%v", node.WorkerID())
			}

			idMap[node.WorkerID()] = "true"

			t.Logf("get id from etcd: %v", node.WorkerID())

		}()
	}

	wg.Wait()

	//time.Sleep(time.Second * 120)

}
