package file

import (
	"albus/pb"
	"albus/utils"
	"os"
	"sync"
)

// 负责对sst文件操作的封装
type SSTable struct {
	lock       *sync.RWMutex
	file       *MmapFile
	maxKey     []byte
	minKey     []byte
	tableIndex *pb.TableIndex
	hasBF      bool
	idxLen     int
	idxStart   int
	fid        uint32
}

func OpenSST(opt *Options) *SSTable {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSize)
	utils.Err(err)
	return &SSTable{
		file: mf,
		fid:  opt.FID,
		lock: &sync.RWMutex{},
	}
}

func (ss *SSTable) Init() error {
	return nil
}

func (ss *SSTable) initTable() (blockoffset *pb.BlockOffset, err error) {
	return nil, nil
}

