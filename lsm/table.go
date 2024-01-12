package lsm

import "albus/file"

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32
}
