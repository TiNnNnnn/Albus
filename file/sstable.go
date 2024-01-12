package file

import "sync"

type SSTable struct {
	lock *sync.RWMutex
	file *MmapFile
}
