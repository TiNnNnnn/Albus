package lsm

import (
	"albus/file"
	"albus/pb"
	"albus/utils"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"

	"github.com/pkg/errors"
)

type table struct {
	sst *file.SSTable
	lm  *levelManager
	fid uint64
}

// mainfest中加载，落盘memtable
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	//开辟一片新的mmap映射区
	sstSize := int(lm.opt.SSTableMaxSize)
	if builder != nil {
		sstSize = int(builder.doneToDisk().size)
	}

	var t *table
	var err error

	//log.Printf("create sstable file,name:%s,fid:%d", tableName, t.fid)
	//b, _ := sst.Bytes(0, int(lm.opt.SSTableMaxSize))
	//log.Printf("new mmapfile:%v,len:%d", b, len(b))

	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		//builder = nil,加载manifest到内存
		sst := file.OpenSST(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkerDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSize:  int(sstSize),
		})
		t = &table{
			sst: sst,
			lm:  lm,
			fid: utils.GetFidByPath(tableName),
		}
	}

	if err := t.sst.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	return t
}

// 从sstables中查找key
func (t *table) Serach(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	tableidx := t.sst.GetIndexs()

	//检查key是否在当前block中
	filter := &utils.Filter{}
	*filter.GetTablePtr() = tableidx.BloomFilter
	if t.sst.HasBloomFilter() && !filter.MayContainKey(utils.ParseKey(key)) {
		//log.Printf("no bloom\n")
		return nil, utils.ErrKeyNotFound
	}

	iter := t.NewTableIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if iter.Valid() {
		log.Printf("[table] seek failed")
		return nil, utils.ErrKeyNotFound
	}
	if utils.IsSameKey(key, iter.Item().Entry().Key) {
		if version := utils.ParseTimeFromKey(iter.Item().Entry().Key); *maxVs < version {
			//log.Printf("table serach key:%s,paresekey:%s\n", key, utils.ParseKey(key))
			//log.Printf("table value: %s\n", iter.Item().Entry().Value)
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) NewTableIterator(options *utils.Options) utils.Iterator {
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}

func (it *tableIterator) Next() {

}

func (it *tableIterator) Valid() bool {
	return it == nil
}

func (it *tableIterator) Rewind() {

}

func (it *tableIterator) Item() utils.Item {
	return it.it
}

func (it *tableIterator) Close() error {
	return nil
}

// 二分搜索block offsets
func (it *tableIterator) Seek(key []byte) {
	var blockOffset pb.BlockOffset
	idx := sort.Search(len(it.t.sst.GetIndexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&blockOffset, idx), fmt.Errorf("table seek idx<0 || idx > len(idx.Getoffsets())"))
		return utils.CompareKeys(blockOffset.GetKey(), key) > 0
	})

	if idx == 0 {
		it.blockseek(0, key)
	}
	it.blockseek(idx-1, key)
	if it.err == io.EOF {
		if idx == len(it.t.sst.GetIndexs().GetOffsets()) {
			return
		}
		it.blockseek(idx, key)
	}
}

// 在sst中第blockIdx个block内找key
func (it *tableIterator) blockseek(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.getBlockFromSST(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	//使用blockiterator进行块内查找
	it.bi.tableId = it.t.fid
	it.bi.blockId = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

// 根据blcokIdx去sst中加载对应block
func (t *table) getBlockFromSST(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx<0,idx = %d", idx))
	utils.CondPanic(idx >= len(t.sst.GetIndexs().GetOffsets()), fmt.Errorf("block out of index,idx = %d", idx))

	var b *block

	//先从blockcache中尝试拿数据
	key := t.makeblockCacheKey(idx)
	blk, ok := t.lm.cache.blockCache.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	var blockOffset pb.BlockOffset
	utils.CondPanic(!t.offsets(&blockOffset, idx), fmt.Errorf("lockOffsets idx = %d", idx))
	b = &block{
		offset: int(blockOffset.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(blockOffset.GetLen())); err != nil {
		return nil, errors.Wrapf(err, "read block %d from sst error,offset:%d,len:%d", t.sst.GetFid(), b.offset, blockOffset.GetLen())
	}
	//对block data进行解析
	readPos := len(b.data) - 4
	b.checkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))

	if b.checkLen > len(b.data) {
		return nil, errors.Errorf("invalid checksum len,checkesum = %d", b.checkLen)
	}
	readPos -= b.checkLen
	b.checkSum = b.data[readPos : readPos+b.checkLen]
	//读取offset_len
	readPos -= 4
	offsetLen := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	//读取offsets
	offsetsStart := readPos - (offsetLen * 4)
	b.entryOffsets = utils.BytesToU32Slice(b.data[offsetsStart:readPos])
	b.entriesIdxStart = offsetsStart

	//tips:对kv_data+offsets进行MD5编码
	//验证块正确性
	b.data = b.data[0 : readPos+4]
	if err = b.compareCheckSum(); err != nil {
		return nil, err
	}

	//加入blockCache
	t.lm.cache.blockCache.Set(key, b)
	return b, nil
}

func (t *table) read(off int, sz int) ([]byte, error) {
	return t.sst.Bytes(off, sz)
}

// 从tableIndex中获取对应的blockOffsets
func (t *table) offsets(bo *pb.BlockOffset, i int) bool {
	index := t.sst.GetIndexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	*bo = *index.GetOffsets()[i]
	return true
}

// 生成访问blcokcache的key
// blockCache,[key:fid_blockoffset] [value: block]
func (t *table) makeblockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t/fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("idx >= math.MaxUint32"))

	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}
