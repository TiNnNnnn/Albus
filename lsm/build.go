package lsm

import "albus/file"

type tableBuilder struct {
	sstSize  int64
	curBlock *block
	//opt      *Options
}

type buildDate struct {
	blockList []*block
	idx       []byte
	checkSum  []byte
	size      int
}

type block struct {
	offset          int
	checkSum        []byte
	checkLen        int
	entriesIdxStart int
	entryOffsets    []uint32
	data            []byte
	baseKey         []byte
	end             int
	estimateSize    int64
}

type header struct {
	overlap uint64
	diff    uint64
}

func (tb *tableBuilder) flush(sst *file.SSTable) error {
	return nil
}
