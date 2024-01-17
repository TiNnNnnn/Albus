package lsm

import (
	"albus/file"
	"albus/pb"
	"albus/utils"
	"bytes"
	"fmt"
	"io"
	"math"
	"sort"
	"unsafe"

	"google.golang.org/protobuf/proto"
)

type tableBuilder struct {
	sstSize     int64
	curBlock    *block
	opt         *Options
	blockList   []*block
	keyCount    uint32
	keyHashList []uint32
	maxVersion  uint64
	baseKey     []byte
}

type buildDate struct {
	//block list
	blockList []*block
	//index_data
	idx []byte
	//checkSum of index_data
	checkSum []byte
	//sst total size
	size int
}

type block struct {
	//block起始地址
	offset int
	//校验和与其长度
	checkSum []byte
	checkLen int
	//block索引起始地址
	entriesIdxStart int
	//offsets数组：记录kv的起始地址
	entryOffsets []uint32
	//暂存储sstable，需要拷贝到mmap数组中
	data []byte
	//baseKey
	baseKey []byte
	//结束地址
	end int
	//预估大小
	estimateSize int64
}

type header struct {
	overlap uint16
	diff    uint16
}

// 策略模式，新建一个sstable builder
func newTableBuilder(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt: opt,
	}
}

// 向tableBuilder中写入entry
func (tb *tableBuilder) add(e *utils.Entry) {
	key := e.Key
	if tb.checkBlockRemainCap(e) {
		//curblock剩余空间不足，进行fill操作，加入block_list，
		//并分配新的block.
		tb.fillBlock()
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize),
		}
	}

	tb.keyHashList = append(tb.keyHashList, utils.Hash(utils.ParseKey(key)))
	version := utils.ParseTimeFromKey(key)
	if version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffkey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffkey = key
	} else {
		diffkey = tb.calKeyDiff(key)
	}
	utils.CondPanic(!(len(key)-len(diffkey) <= math.MaxUint16), fmt.Errorf("len(key)-len(diffkey) > math.MaxUint16"))
	utils.CondPanic(!(len(diffkey) <= math.MaxUint16), fmt.Errorf("en(diffkey)>math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffkey)),
		diff:    uint16(len(diffkey)),
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))
	tb.pushBack(h.encodeHead())
	tb.pushBack(diffkey)

	emptybuf := tb.allocate(int(e.EncodeSize()))
	e.EncodeEntry(emptybuf)
}

// 计算key与basekey的diff部分
func (tb *tableBuilder) calKeyDiff(key []byte) []byte {
	i := 0
	for ; i < len(key) && i < len(tb.curBlock.baseKey); i++ {
		if key[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return key[i:]
}

// 检查当前block剩余空间是否足够再添加entry
func (tb *tableBuilder) checkBlockRemainCap(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	utils.CondPanic(!(uint32(len(tb.curBlock.entryOffsets)+1)*4+4+8+4 < math.MaxUint32), utils.ErrOutofInterger)
	curblockSize := uint32(tb.curBlock.end) + uint32(uint32(len(tb.curBlock.entryOffsets)+1)*4) + 4 + 8 + 4
	estimateSize := curblockSize + uint32(6) + uint32(len(e.Key)) + uint32(e.EncodeSize())
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(estimateSize) < math.MaxUint32), utils.ErrOutofInterger)
	return estimateSize > uint32(tb.opt.BlockSize)
}

func (tb *tableBuilder) fillBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	tb.pushBack(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.pushBack(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checkSum := tb.calCheckSum(tb.curBlock.data[:tb.curBlock.end])
	tb.pushBack(checkSum)
	tb.pushBack(utils.U32ToBytes(uint32(len(checkSum))))

	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
}

func (tb *tableBuilder) calCheckSum(data []byte) []byte {
	return utils.U64ToBytes(utils.CalCheckSum(data))
}

// 向curBlock.data追加数据
func (tb *tableBuilder) pushBack(data []byte) {
	emptyBuf := tb.allocate(len(data))
	l := copy(emptyBuf, data)
	utils.CondPanic(len(data) != l, utils.ErrBadBuilderPushBack)
}

func (tb *tableBuilder) allocate(need int) []byte {
	curb := tb.curBlock
	if len(curb.data[curb.end:]) < need {
		sz := 2 * len(curb.data)
		if curb.end+need > sz {
			sz = curb.end + need
		}
		//TODO 使用arena代替
		tmp := make([]byte, sz)
		copy(tmp, curb.data)
		curb.data = tmp
	}
	curb.end += need
	return curb.data[curb.end-need : curb.end]
}

func (h *header) headerSize() uint16 {
	return uint16(unsafe.Sizeof(header{}))
}

func (h *header) encodeHead() []byte {
	var buf [4]byte
	*(*header)(unsafe.Pointer(&buf[0])) = *h
	return buf[:]
}

func (h *header) decodeHead(buf []byte) {
	const hz = uint16(unsafe.Sizeof(header{}))
	dst := ((*[hz]byte)(unsafe.Pointer(h))[:])
	src := buf[:hz]
	copy(dst, src)
}

// TODO：可以优化拷贝次数
// 将blocklist落盘为sstable
func (tb *tableBuilder) flush(sst *file.SSTable) error {
	builddata := tb.doneToDisk()
	//创建一个buf,并将bd中所有数据拷贝进去
	buf := make([]byte, builddata.size)
	written := builddata.CopyData(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("copydata from buildData failed"))
	//拷贝数据到mmap内存映射区域
	dst, err := sst.Bytes(0, builddata.size)
	if err != nil {
		return err
	}
	copy(dst, buf)
	return nil
}

// 将idx区域数据顺序写入临时切片
func (bd *buildDate) CopyData(dst []byte) int {
	var written int
	for _, block := range bd.blockList {
		written += copy(dst[written:], block.data[:block.end])
	}
	written += copy(dst[written:], bd.idx)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.idx))))
	written += copy(dst[written:], bd.checkSum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checkSum))))
	return written
}

func (tb *tableBuilder) doneToDisk() buildDate {
	tb.fillBlock()
	if len(tb.blockList) == 0 {
		return buildDate{}
	}
	bd := buildDate{
		blockList: tb.blockList,
	}

	//创建bloom_filter并写入所有key
	var newfilter *utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bpk := utils.BitsPerKey(len(tb.keyHashList), tb.opt.BloomFalsePositive)
		newfilter = utils.NewFilterByKeys(tb.keyHashList, bpk)
	}

	index, dataSize := tb.buildIndex(newfilter.GetTable())
	//为索引区域计算crc32校验和
	checkSum := tb.calCheckSum(index)
	bd.idx = index
	bd.checkSum = checkSum
	bd.size = int(dataSize) + len(index) + len(checkSum) + 4 + 4

	return bd
}

// 构建sst index_data区域
func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableidx := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableidx.BloomFilter = bloom
	}
	tableidx.KeyCount = tb.keyCount
	tableidx.MaxVersion = tb.maxVersion
	tableidx.Offsets = tb.buildBlockOffsets()
	//计算所有block总大小
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := proto.Marshal(tableidx)
	utils.Panic(err)
	return data, dataSize
}

// 构建index_data中的block_offsets
func (tb *tableBuilder) buildBlockOffsets() []*pb.BlockOffset {
	var startOffset uint32
	var blockOffsets []*pb.BlockOffset

	for _, block := range tb.blockList {
		blockOffset := tb.writeBlockOffset(block, startOffset)
		blockOffsets = append(blockOffsets, blockOffset)
		startOffset += uint32(block.end)
	}
	return blockOffsets
}

// 构建block_offset
func (tb *tableBuilder) writeBlockOffset(block *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{
		Key:    block.baseKey,
		Len:    uint32(block.end),
		Offset: startOffset,
	}
	return offset
}

type blockIterator struct {
	//block entry_data数据
	data []byte
	//迭代器当前位置
	idx int
	//err
	err error
	//basekey
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32

	block   *block
	tableId uint64
	blockId int

	prevOverlap uint16
	it          utils.Item
}

func (it *blockIterator) setBlock(b *block) {
	it.block = b
	it.err = nil
	it.idx = 0
	it.baseKey = b.baseKey[:0]
	it.prevOverlap = 0
	it.key = it.key[:0]
	it.val = it.val[:0]
	it.data = b.data[:b.entriesIdxStart]
	it.entryOffsets = b.entryOffsets
}

func (it *blockIterator) seek(key []byte) {
	it.err = nil
	startidx := 0
	//从entryOffsets中二分查找taget key
	tagetEntryIdx := sort.Search(len(it.entryOffsets), func(idx int) bool {
		if idx < startidx {
			return false
		}
		//移动到idx位置
		it.setIdx(idx)
		//it.key>key: key在it.key左半部分或者当前位置
		//it.key>key: key在it.key右半部分
		return utils.CompareKeys(it.key, key) >= 0
	})
	it.setIdx(tagetEntryIdx)
}

func (it *blockIterator) setIdx(i int) {
	defer func() {
		//recover: 捕获并处理panic异常，只能在defer中使用
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			panic(debugBuf.String())
		}
	}()

	it.idx = i
	if i > len(it.entryOffsets) || i < 0 {
		it.err = io.EOF
		return
	}
	it.err = nil

	//寻找当前block的startoffset和endoffset,从而
	//反序列化出entry相关信息
	startOffset := int(it.entryOffsets[i])

	if len(it.baseKey) == 0 {
		var baseHeader header
		baseHeader.decodeHead(it.data)
		headerSz := baseHeader.headerSize()
		it.baseKey = it.data[headerSz : headerSz+baseHeader.diff]
	}

	var endOffset int
	if it.idx+1 == len(it.entryOffsets) {
		//当前是最后一个block
		endOffset = len(it.data)
	} else {
		//当前不是最后一块
		endOffset = int(it.entryOffsets[it.idx+1])
	}

	entryData := it.data[startOffset:endOffset]
	var h header
	h.decodeHead(entryData)
	if h.overlap > it.prevOverlap {
		it.key = append(it.key[:it.prevOverlap], it.baseKey[it.prevOverlap:h.overlap]...)
	}

	it.prevOverlap = h.overlap
	valueOff := h.headerSize() + h.diff
	diffKey := entryData[h.headerSize():valueOff]
	it.key = append(it.key[:h.overlap], diffKey...)
	e := utils.NewEntry(it.key, nil)
	e.DecodeEntry(entryData[valueOff:])
	it.it = &Item{
		e: e,
	}
}

func (it *blockIterator) Next() {
	it.setIdx(it.idx + 1)
}

func (it *blockIterator) Rewind() bool {
	it.setIdx(0)
	return true
}

func (it *blockIterator) Item() utils.Item {
	return it.it
}

func (it *blockIterator) Valid() bool {
	return it.it == nil
}

func (it *blockIterator) Error() error {
	return it.err
}

func (it *blockIterator) Close() error {
	return nil
}
