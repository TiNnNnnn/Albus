package lsm

import "albus/utils"

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
}

type Options struct {
	WorkerDir          string
	MemTableSize       int64
	SSTableMaxSize     int64
	BlockSize          int
	BloomFalsePositive float64
	
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{
		option: opt,
	}
	return lsm
}

func (lsm *LSM) Close() error {
	if lsm.memTable != nil {
		
	}
	return nil
}
