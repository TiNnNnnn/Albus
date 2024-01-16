package lsm

import (
	"albus/utils"
	"sync"
	"sync/atomic"
)

type levelManager struct {
	maxFid      uint64
	opt         *Options
	cache       *LsmCache
	maifestFile uint64
	levels      []*levelHandler
}

// 负责对levelNum层的sstables进行操作
type levelHandler struct {
	sync.RWMutex
	levelNum int
	tables   []*table
}

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) add(t *table) {
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if lh.levelNum == 0 {
		//TODO ...
		return nil, nil
	} else {
		return nil, nil
	}
}

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {

	} else {

	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	return nil, nil
}

func (lh *levelHandler) searchLnSST(key []byte) (*utils.Entry, error) {
	return nil, nil
}

func (lh *levelHandler) getTable(key []byte) *table {
	return nil
}

func newlevelManager(opt *Options) *levelManager {
	return nil
}

func (lm *levelManager) close() error {
	return nil
}

func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	return nil, nil
}

func (lm *levelManager) loadCache() {

}

func (lm *levelManager) loadManifest() error {
	return nil
}

func (lm *levelManager) build() error {
	return nil
}

// 向L0层flush一个sstable
func (lm *levelManager) flush(immutable *memTable) error {
	//分配一个fid
	nextId := atomic.AddUint64(&lm.maxFid, 1)
	newSSTName := utils.GenSSTPath(lm.opt.WorkerDir, nextId)

	//构建一个builder对象
	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry)
	}
	//创建一个sstable对象
	table := openTable(lm, newSSTName, builder)

	//更新manifest文件
	//TODO:maifest
	lm.levels[0].add(table)
	return nil
}
