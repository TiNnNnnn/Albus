package file

import "sync"

type WalFile struct {
	lock *sync.RWMutex
	f    *MmapFile
}
