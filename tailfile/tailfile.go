package tailfile

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"goLoggerTest/logagent/kafka"
	"strings"
	"time"
)

type tailTask struct {
	path   string
	topic  string
	tObj   *tail.Tail
	ctx    context.Context
	cancel context.CancelFunc
}

func newTailTask(path, topic string) *tailTask {
	ctx, cancel := context.WithCancel(context.Background())
	tt := &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
	}
	return tt
}

func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	t.tObj, err = tail.TailFile(t.path, cfg)
	return
}

func (t *tailTask) run() {
	//读取日志，发往Kafka
	logrus.Info("collect for path:%s is running...", t.path)
	for {
		select {
		case <-t.ctx.Done():
			logrus.Infof("path:%s is stopping...", t.path)
			return
		case line, ok := <-t.tObj.Lines: //chan tail.Line
			if !ok {
				logrus.Warn("tail file close reopen,path:%s\n", t.path)
				time.Sleep(time.Second)
				continue
			}
			//如果是空行，就跳过
			if len(strings.TrimSpace(line.Text)) == 0 {
				logrus.Info("出现空行了，跳过")
				continue
			}
			//利用通道将同步的代码改为异步的
			//把都出来的一行日志保诚成Kafka里面的msg类型，丢到通道中
			msg := &sarama.ProducerMessage{}
			msg.Topic = t.topic
			msg.Value = sarama.StringEncoder(line.Text)
			kafka.ToMsgChan(msg)
		}
	}
}
