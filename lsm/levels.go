package lsm

import (
	"albus/file"
	"albus/utils"
	"bytes"
	"log"
	"sort"
	"sync"
	"sync/atomic"
)

type levelManager struct {
	maxFid       uint64
	opt          *Options
	cache        *LsmCache
	manifestFile *file.ManifestFile
	levels       []*levelHandler
}

// 负责对levelNum层的sstables进行操作
type levelHandler struct {
	sync.RWMutex
	levelNum int
	tables   []*table
}

func newlevelManager(opt *Options) *levelManager {
	lm := &levelManager{}
	lm.opt = opt
	//读取manifast文件构建manager
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.buildManager()
	return lm
}

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) add(t *table) {
	log.Printf("level_%d append new sst_%d", lh.levelNum, t.fid)
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	//log.Printf("levelhandler level:%d\n", lh.levelNum)
	if lh.levelNum == 0 {
		//TODO ...
		return lh.searchL0SST(key)
	} else {
		return lh.searchLnSST(key)
	}
}

// 对sst进行排序
func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		//L0层，block的minkey和maxkey存在交集，直接按照sst fid排序即可
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		//L1+层，merge之后block之间无交集，按照minkey排序
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].sst.GetMinKey(), lh.tables[j].sst.GetMinKey()) < 0
		})
	}
}

// 在L0层搜索key
func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Serach(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

// 在Ln层搜索key
func (lh *levelHandler) searchLnSST(key []byte) (*utils.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if entry, err := table.Serach(key, &version); err != nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}

// 获取key所在的table
func (lh *levelHandler) getTable(key []byte) *table {
	for i := len(lh.tables) - 1; i >= 0; i-- {
		if bytes.Compare(key, lh.tables[i].sst.GetMinKey()) > -1 && bytes.Compare(key, lh.tables[i].sst.GetMaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		log.Println("levelmanager close cache error,err:", err)
		return err
	}
	if err := lm.manifestFile.Close(); err != nil {
		log.Println("levelmanar close manifestFile error,err:", err)
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			log.Println("levelmanar close  levelhandler error,err:", err)
			return err
		}
	}
	return nil
}

func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	var entry *utils.Entry
	var err error
	//查询LO层
	if entry, err = lm.levels[0].Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}
	//查询L1+层
	for level := 1; level < utils.MaxLevelNum; level++ {
		if entry, err = lm.levels[level].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	return entry, utils.ErrKeyNotFound
}

func (lm *levelManager) loadCache() {
	lm.cache = newLsmCache(lm.opt)
	//初始化idxcache
	//key:fid,value:table
	for _, level := range lm.levels {
		for _, table := range level.tables {
			lm.cache.setIndex(table.sst.GetFid(), table)
		}
	}
}

func (lm *levelManager) loadManifest() error {
	var err error
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{
		Dir: lm.opt.WorkerDir,
	})
	return err
}

func (lm *levelManager) buildManager() error {
	//构建各层levelHandler
	lm.levels = make([]*levelHandler, 0, utils.MaxLevelNum)
	for i := 0; i < utils.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
		})
	}

	//检测manifest文件的正确性,并进行统一
	if err := lm.manifestFile.RevertToManifest(utils.LoadIdMap(lm.opt.WorkerDir)); err != nil {
		return err
	}

	//构建level
	var maxFid uint64
	manifest := lm.manifestFile.GetManifest()
	for fid, tableInfo := range manifest.Tables {
		fileName := utils.GenSSTPath(lm.opt.WorkerDir, fid)
		if fid > maxFid {
			maxFid = fid
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].tables = append(lm.levels[tableInfo.Level].tables, t)
	}
	//对各层进行排序
	for i := 0; i < utils.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	lm.maxFid = maxFid
	lm.loadCache()
	return nil
}

// 向L0层flush一个sstable
func (lm *levelManager) flush(immutable *memTable) error {
	log.Printf("immutables exist,begin flush data to sstable...")
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

	lm.levels[0].add(table)
	//更新manifest文件
	err := lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		Id:       nextId,
		CheckSum: utils.MagicText[:],
	})
	utils.CondPanic(err != nil, err)
	//flush成功之后，关闭immutable
	immutable.close()

	return nil
}
