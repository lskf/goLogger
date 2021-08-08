package tailfile

import (
	"github.com/sirupsen/logrus"
	"goLoggerTest/logagent/common"
)

//tailTask 的管理者

type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask       //所有的tailTask任务
	collectEntryList []common.CollectEntry      //所有配置项
	confChan         chan []common.CollectEntry //等待新配置的通道
}

var (
	ttMgr *tailTaskMgr
)

func Init(allConf []common.CollectEntry) (err error) {
	//allConf 里面存了若干个日志的收集项
	//针对每一个日志收集项创建一个对应的tailObj
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry),
	}
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic) //创建一个日志收集任务
		err = tt.Init()                          //打开日志文件准备读
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
			return
		}
		logrus.Info("create a tail task for path :%s success", conf.Path)
		ttMgr.tailTaskMap[tt.path] = tt //把创建的这个tailTask任务登记在册，方便后续管理
		// 起一个后台的goroutine收集日志
		go tt.run()
	}
	go ttMgr.watch() // 后台等待行的配置来
	return
}

func (t *tailTaskMgr) watch() {
	for {
		// 开启一个协程等待新配置来
		newConf := <-t.confChan //取到值说明新配置已经获取到了
		logrus.Info("get new conf from etcd, conf:%v, start manage tailTask...", newConf)
		for _, conf := range newConf {
			// 1. 新配置有的，原本就存在的话，就不用动
			if t.isExist(conf) {
				continue
			}
			// 2. 新配置有的，原本没有的，就创建
			tt := newTailTask(conf.Path, conf.Topic) //创建一个日志收集任务
			err := tt.Init()                         //打开日志文件准备读
			if err != nil {
				logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
				return
			}
			logrus.Info("create a tail task for path :%s success", conf.Path)
			t.tailTaskMap[tt.path] = tt //把创建的这个tailTask任务登记在册，方便后续管理
			// 起一个后台的goroutine收集日志
			go tt.run()
		}
		// 3. 新配置没有的，原本有的，就把tailTask停掉
		// 找出tailTaskMap 中存在，newConf不存在的那些tailTask,把它们停掉
		for key, task := range t.tailTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Path {
					found = true
					break
				}
			}
			if !found {
				//这个tailTask要停掉了
				logrus.Infof("the task collect path:%s need to stop.", task.path)
				delete(t.tailTaskMap, key) //将停掉的tailTask 从tailTaskMap中删掉
				task.cancel()
			}
		}
	}
}

// 判断tailTaskMap中是否存在该收集项
func (t *tailTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}

func SendNewConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}
