package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"goLoggerTest/logagent/common"
	"goLoggerTest/logagent/etcd"
	"goLoggerTest/logagent/kafka"
	"goLoggerTest/logagent/tailfile"
	"gopkg.in/ini.v1"
)

type Config struct {
	KafkaConfig `ini:"kafka"`
	//CollectConfig `ini:"collect"`
	EtcdConfig `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}

//type CollectConfig struct {
//	LogFilePath string `ini:"logfile_path"`
//}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

//日志收集的客户端
//类似的开源项目还有filebeat
//手机指定目录下的日志文件，发送到kafka中
func run() {
	select {}
}

func main() {
	// -1：获取本机IP，为后续去etcd取配置大侠基础
	ip, err := common.GetOutboundIP()
	if err != nil {
		logrus.Errorf("get ip failed,err:%v", err)
		return
	}

	var configObj = new(Config)
	// 0.读配置文件 `go-ini`

	// cfg, err := ini.Load("./conf/config.ini")
	// if err != nil {
	// 	logrus.Error("load config failed,err:%v", err)
	// 	return
	// }
	// kafkaAddr := cfg.Section("kafka").Key("address").String()
	// fmt.Println(kafkaAddr)

	err = ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Error("load config failed,err:%v", err)
		return
	}

	// 1.初始化连接kafka（做好准备工作）
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Error("init kafka failed,err:%v", err)
		return
	}
	logrus.Info("init kafka success!")

	//初始化etcd连接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Error("init etcd failed,err:%v", err)
		return
	}
	logrus.Info("init etcd success!")
	//从etcd中拉去要收集日志的配置项
	collectKey := fmt.Sprintf(configObj.EtcdConfig.CollectKey, ip)
	allConf, err := etcd.GetConf(collectKey)
	if err != nil {
		logrus.Error("get etcd conf failed,err:%v", err)
		return
	}
	//开一个协程去监听etcd中的configObj.EtcdConfig.CollectKey的变化
	go etcd.WatchConf(collectKey)

	// 2.根据配置中的日志路径初始化tail包
	err = tailfile.Init(allConf) //把从etcd 中获取的配置项传到init中
	if err != nil {
		logrus.Error("init tailFile failed,err:%v", err)
		return
	}
	logrus.Info("init tailFile success!")
	run()
}
