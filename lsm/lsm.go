package lsm

import (
	"albus/utils"
	"log"
)

type LSM struct {
	memTable *memTable
	//immutables: memtable list
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
	//最大memtable编号
	maxMemFid uint32
}

// 可选选项
type Options struct {
	//LSM工作目录
	WorkerDir          string
	MemTableSize       int64
	SSTableMaxSize     int64
	BlockSize          int
	BloomFalsePositive float64
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{
		option: opt,
	}
	//启动DB恢复，加载wal,如果未恢复则创建新内存表
	lsm.memTable, lsm.immutables = lsm.recovery()
	//初始化levelManager
	lsm.levels = newlevelManager(opt)
	//初始化closer,用于资源回收信号控制
	lsm.closer = utils.NewCloser(1)
	log.Printf("new lsm success")
	return lsm
}

func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	//检查当前memtable是否写满，是则创建新的memtable,同时将当前
	//memtable加入immutable
	if lsm.memTable.Size() > lsm.option.MemTableSize {
		lsm.immutables = append(lsm.immutables, lsm.memTable)
		lsm.memTable = lsm.Newmemtable()
		log.Printf("cur memtable has full, create new memtable,append cur mmetable into immutables,size: %d\n", lsm.memTable.Size())
	}
	//向memtable中插入数据
	if err := lsm.memTable.set(entry); err != nil {
		return err
	}
	//fmt.Printf("now memtable size:%d\n",lsm.memTable.Size())

	//检查是否有immutable是否需要落盘
	for _, immutable := range lsm.immutables {
		if err := lsm.levels.flush(immutable); err != nil {
			return err
		}
	}

	//释放immutable表
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*memTable, 0)
	}
	return nil
}

func (lsm *LSM) Close() error {
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			log.Println("lsm close memtable error,err:", err)
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			log.Println("lsm close immutables error,err:", err)
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	//等待合并过程结束
	lsm.closer.Close()
	return nil
}

func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	var entry *utils.Entry
	var err error
	//查询memtable，先查询内存活跃表，再查内存不变表
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		log.Printf("[get entry from memtable success]\n")
		return entry, err
	}
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err := lsm.immutables[i].Get(key); entry != nil && entry.Value != nil {
			log.Printf("[get entry from immutables success]\n")
			return entry, err
		}
	}
	//从磁盘查询
	return lsm.levels.Get(key)
}

func (lsm *LSM) StartMerge() {
	defer lsm.closer.Done()
	for {
		select {
		case <-lsm.closer.Wait():
			return
		}
		//TODO:处理并发的合并过程
	}
}
