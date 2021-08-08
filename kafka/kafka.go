package kafka

import (
	"github.com/sirupsen/logrus"

	"github.com/Shopify/sarama"
)

//kafka 相关操作

var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

//init 是初始化全局的kafka 连接
func Init(address []string, chanSize int64) (err error) {
	//1. 生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          //ack
	config.Producer.Partitioner = sarama.NewRandomPartitioner //分区
	config.Producer.Return.Successes = true                   //确认

	//2.连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("kafka: producer closed, err:", err)
		return
	}
	//初始化MsgChan
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	//defer client.Close()
	//起一个后台的goroutine从MsgChan 中读数据
	go sendMsg()
	return
}

//从MsgChan中读取msg，发送给Kafka
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed ,err:", err)
				return
			}
			logrus.Info("send msg to kafka success.pid:%v offset:%v", pid, offset)
		}
	}
}

//定义一个函数向外暴露msgChan
func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
