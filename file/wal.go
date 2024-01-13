package file

import (
	"albus/utils"
	"sync"
)

type WalFile struct {
	lock *sync.RWMutex
	f    *MmapFile
}

func (wf *WalFile) Close() error {
	return nil
}

func OpenWalFile(opt *Options) *WalFile {
	return nil
}

func (wf *WalFile) Write(entry *utils.Entry) error {
	return nil
}
