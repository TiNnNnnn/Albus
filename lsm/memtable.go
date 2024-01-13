package lsm

import (
	"albus/file"
	"albus/utils"
)

type memTable struct {
	wal *file.WalFile
	sl  *utils.SkipList
}

func Newmemtable() (*memTable, error) {
	return nil, nil
}

func (m *memTable) close() error {
	return nil
}
