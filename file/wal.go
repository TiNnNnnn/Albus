package file

import (
	"albus/utils"
	"os"
	"sync"
)

type WalFile struct {
	lock *sync.RWMutex
	f    *MmapFile
}

func (wf *WalFile) Close() error {
	if err := wf.f.Close(); err != nil {
		return err
	}
	return nil
}

func OpenWalFile(opt *Options) *WalFile {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSize)
	utils.Err(err)
	return &WalFile{
		f:    mf,
		lock: &sync.RWMutex{},
	}
}

func (wf *WalFile) Write(entry *utils.Entry) error {
	//TODO
	
	return nil
}
