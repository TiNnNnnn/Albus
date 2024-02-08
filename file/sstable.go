package file

import (
	"albus/pb"
	"albus/utils"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

// 负责对sst文件操作的封装
type SSTable struct {
	lock       *sync.RWMutex
	mmapfile   *MmapFile
	maxKey     []byte
	minKey     []byte
	tableIndex *pb.TableIndex
	hasBF      bool
	idxLen     int
	idxStart   int
	fid        uint32
	createAt time.Time
}

// 打开一个sst文件
func OpenSST(opt *Options) *SSTable {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSize)
	utils.Err(err)
	return &SSTable{
		mmapfile: mf,
		fid:      opt.FID,
		lock:     &sync.RWMutex{},
	}
}

// Init 初始化 (sst index 磁盘-->内存)
func (ss *SSTable) Init() error {
	var blockOffset *pb.BlockOffset
	var err error
	if blockOffset, err = ss.initTable(); err != nil {
		return err
	}

	//计算minkey
	keyBytes := blockOffset.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey

	//计算maxkey(tips:最后一个block的basekey)
	blockLen := len(ss.tableIndex.Offsets)
	blockOffset = ss.tableIndex.Offsets[blockLen-1]
	keyBytes = blockOffset.GetKey()
	maxKey := make([]byte, 0)
	copy(maxKey, keyBytes)
	ss.maxKey = maxKey
	return nil
}

func (ss *SSTable) initTable() (blockoffset *pb.BlockOffset, err error) {
	//解析sst 索引区 （倒序解析）
	pos := len(ss.mmapfile.Data)
	//log.Printf("init, mmap data: %v", ss.mmapfile.Data)
	//读取checkSum len
	pos -= 4
	buf := ss.readWithCheck(pos, 4)
	checksumLen := int(utils.BytesToU32(buf))
	//log.Printf("init, buf: %v,checkSum len %d", buf, checksumLen)
	if checksumLen < 0 {
		return nil, errors.New("pasre checksum len from mmap error,len <0")
	}
	//读取checkSum
	pos -= checksumLen
	checkSumBytes := ss.readWithCheck(pos, checksumLen)
	//log.Printf("checkSumBytes len: %d", len(checkSumBytes))
	//读取 index size
	pos -= 4
	buf = ss.readWithCheck(pos, 4)
	ss.idxLen = int(utils.BytesToU32(buf))
	//read index data
	pos -= ss.idxLen
	//log.Printf("buf len: %d", len(buf))
	ss.idxStart = pos
	idxData := ss.readWithCheck(pos, ss.idxLen)

	if err := utils.CompareCheckSum(idxData, checkSumBytes); err != nil {
		return nil, err
	}
	tableIndex := &pb.TableIndex{}
	if err := proto.Unmarshal(idxData, tableIndex); err != nil {
		return nil, err
	}
	ss.tableIndex = tableIndex
	ss.hasBF = len(tableIndex.BloomFilter) > 0
	if len(tableIndex.GetOffsets()) > 0 {
		//返回第一个blockoffest
		return tableIndex.GetOffsets()[0], nil
	}
	return nil, errors.New("read idx error: offset data is nil")
}

func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBF
}

func (ss *SSTable) Bytes(off int, sz int) ([]byte, error) {
	return ss.mmapfile.Bytes(off, sz)
}

func (ss *SSTable) GetIndexs() *pb.TableIndex {
	return ss.tableIndex
}

func (ss *SSTable) GetMaxKey() []byte {
	return ss.maxKey
}

func (ss *SSTable) GetMinKey() []byte {
	return ss.minKey
}

func (ss *SSTable) GetFid() uint32 {
	return ss.fid
}

// 读取mmap映射区 [off,off+sz],并在检测读取错误
func (ss *SSTable) readWithCheck(off int, sz int) []byte {
	buf, err := ss.read(off, sz)
	utils.Panic(err)
	return buf
}

func (ss *SSTable) read(off int, sz int) ([]byte, error) {
	if len(ss.mmapfile.Data) > 0 {
		//读越界，超多mmap长度
		if len(ss.mmapfile.Data[off:]) < sz {
			return nil, io.EOF
		}
		//log.Printf("read mmapfile success,[%d:%d],%s", off, off+sz, ss.mmapfile.Data[off:off+sz])
		return ss.mmapfile.Data[off : off+sz], nil
	}
	//mmap为空，创建新的缓冲区，读取磁盘 【保底策略】
	res := make([]byte, sz)
	_, err := ss.mmapfile.Fd.ReadAt(res, int64(off))
	return res, err
}

func (ss *SSTable)Size()int64{
	fileStatus,err := ss.mmapfile.Fd.Stat()
	utils.Panic(err)
	return fileStatus.Size()
}

func(ss *SSTable)GetCreateAt() *time.Time{
	return &ss.createAt
}

func(ss *SSTable)Delete() error{
	return ss.mmapfile.Delete()
}

func (ss *SSTable)Truncature(size int64) error{
	return ss.mmapfile.Truncature(size)
}