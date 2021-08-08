package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"goLoggerTest/logagent/common"
	"goLoggerTest/logagent/tailfile"
	"time"
)

//etcd 相关操作

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logrus.Error("connect to etcd failed, err:%v\n", err)
		return
	}
	return
}

//拉取日志收集配置项的函数
func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Error("get conf from etcd by key:%s failed,err:%v\n", key, err)
		return
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len:0 conf from etcd by key:%s \n", key)
		return
	}
	ret := resp.Kvs[0]
	fmt.Println(ret.Value)
	err = json.Unmarshal(ret.Value, &collectEntryList)
	if err != nil {
		logrus.Errorf("json unmarshal failed,err:%v", err)
		return
	}
	return
}

//监控etcd中日志收集项的配置变化
func WatchConf(key string) {
	for {
		watchChan := client.Watch(context.Background(), key)
		for wresp := range watchChan {
			logrus.Info("get new conf etcd!")
			for _, evt := range wresp.Events {
				fmt.Printf("type:%s key:%s value:%s\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
				var newConf []common.CollectEntry
				if evt.Type == clientv3.EventTypeDelete {
					//如果是删除
					logrus.Warningf("etcd delete the key !!!")
					tailfile.SendNewConf(newConf)
					continue
				}
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					logrus.Errorf("json unmarshal new conf failed, err:%v", err)
					continue
				}
				//告诉tailfile 这个模块启用行的配置
				tailfile.SendNewConf(newConf)
			}
		}
	}
}
