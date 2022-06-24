package snowflakehelper

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	workerNodePrefix = "_worker"         //节点key前缀， 节点值为CurrentWorkNodeNum, ex: servicename_worker/1, servicename_worker/2
	lockKeyPrefix    = "_snowflake_lock" //锁的名字， ex: servicename__snowflake_lock
	dialTimeout      = 2 * time.Second
	businessTimeout  = 5 * time.Second
	leaseTime        = int64(60) //租期时间 60s
	maxNodeNum       = 10240
)

//GenerateSnowFlakeWorkerID 产生一个workerID
func GenerateSnowFlakeWorkerID(config *clientv3.Config, serviceName string) (int64, error) {
	workerNodeDir := serviceName + workerNodePrefix

	lockDir := serviceName + lockKeyPrefix

	client, err := clientv3.New(*config)
	if err != nil {
		//glog.Errorf("create client err:%v", err)
		return 0, err
	}

	//get a lock
	session, err := concurrency.NewSession(client)
	if err != nil {
		//glog.Errorf("concurrency.NewSession error:%v", err)
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), businessTimeout)
	defer cancel()

	//需要一把锁，防止多个进程重启获得相同的workerId
	m := concurrency.NewMutex(session, lockDir)
	if err := m.Lock(ctx); err != nil {
		//glog.Errorf("lock fail:%v", err)
		return 0, err
	}
	defer m.Unlock(ctx)

	ctx2, cancel := context.WithTimeout(context.Background(), businessTimeout)
	defer cancel()

	resp, err := client.Get(ctx2, workerNodeDir, clientv3.WithPrefix()) //获取所有前缀为worker的节点
	if err != nil {
		return 0, errors.New("get prefix worker node error")
	}
	existNodeMap := make(map[int]int) //定义一个map，保存已经存在的节点
	for _, ev := range resp.Kvs {
		num, _ := strconv.Atoi(string(ev.Value))
		existNodeMap[num] = num //put到existNodeMap中
		//glog.V(8).Infof("%s, %s", ev.Key, ev.Value)
	}
	count := 1
	var validWorkNodeNum int //从1到1024找最小的number
	for ; count < maxNodeNum+1; count++ {
		if _, ok := existNodeMap[count]; !ok { //如果不存在，就会直接break
			validWorkNodeNum = count
			break
		}
	}
	if count == maxNodeNum+1 { //代表maxNodeNum个节点都已经用完了，或者部分节点已经挂掉了，然后key的租期还没有结束，可以重新启动
		return 0, errors.New("服务节点数目大于maxNodeNum")
	}
	err = activeCurrentWorkerNode(client, validWorkNodeNum, workerNodeDir) //启动一个协程一直激活当前key,如果当前服务挂了，key就会在租期结束后查询不到了
	if err != nil {
		//glog.Errorf("activeCurrentWorkerNode error:%v", err)
		return 0, err
	}

	return int64(validWorkNodeNum), nil
}

//InitSnowFlake ...
func InitSnowFlake(config *clientv3.Config, serviceName string) (*Node, error) {

	currentWorkNodeNum, err := GenerateSnowFlakeWorkerID(config, serviceName)
	if err != nil {
		return nil, err
	}

	instance, err := NewNode(currentWorkNodeNum)

	if err != nil {
		//glog.Errorf("create snowflake error:%v", err)
		return nil, err
	}

	return instance, nil
}

func activeCurrentWorkerNode(client *clientv3.Client, workerID int, workerNodeDir string) error {
	lease := clientv3.NewLease(client)
	//glog.V(8).Infof("active currerntNode : %v", workerID)

	ctx, cancel := context.WithTimeout(context.Background(), businessTimeout)
	defer cancel()

	leaseRes, err := lease.Grant(ctx, leaseTime)
	if err != nil {
		//glog.Errorf("Grant error:%v", err)
		return err
	}

	key := fmt.Sprintf("%v/%v", workerNodeDir, workerID)
	val := fmt.Sprintf("%v", workerID)

	ctx, cancel = context.WithTimeout(context.Background(), businessTimeout)
	defer cancel()

	_, err = client.Put(ctx, key, val, clientv3.WithLease(leaseRes.ID))
	if err != nil {
		//glog.Errorf("Put error:%v", err)
		return err
	}

	//glog.V(8).Infof("activeCurrentWorkerNode key:%v  val:%v  success!", key, val)

	leaseRespChan, err := client.KeepAlive(context.Background(), leaseRes.ID)

	if err != nil {
		//glog.Errorf("keepalive error:%v", err)
		return err
	}

	go func() {
		for _ = range leaseRespChan {
			//glog.V(10).Infof("续约成功  %v", leaseKeepResp)
		}
		//glog.V(10).Infof("关闭续租")
	}()

	return nil
}
