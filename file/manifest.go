package file

import (
	"os"
	"sync"
)

// TODO: sst lab
type MainfestFile struct {
	opt  *Options
	f    *os.File
	lock sync.Mutex
	//覆写发生的阈值
	deletionsRewriterThreshold int
	//manifest状态机
	mainfest *Manifest
}

type Manifest struct {
	//保存每一层的对应sst
	Levels []levelManifest
	//用于快速查询一个table位于那一层
	Tables map[uint64]TableManifest
	//统计sst创建次数
	Creations int
	//统计sst删除次数
	Deletions int
}

// sstable的元数据
type TableManifest struct {
	Level    uint8
	checksum []byte
}

type levelManifest struct {
	Tables map[uint64]struct{}
}

func (mf *MainfestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}
