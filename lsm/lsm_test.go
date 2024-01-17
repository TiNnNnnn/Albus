package lsm

import "albus/utils"

var (
	// case
	entrys = []*utils.Entry{
		{Key: []byte("hello0_12345678"), Value: []byte("world0"), ExpirationT: uint64(0)},
		{Key: []byte("hello1_12345678"), Value: []byte("world1"), ExpirationT: uint64(0)},
		{Key: []byte("hello2_12345678"), Value: []byte("world2"), ExpirationT: uint64(0)},
		{Key: []byte("hello3_12345678"), Value: []byte("world3"), ExpirationT: uint64(0)},
		{Key: []byte("hello4_12345678"), Value: []byte("world4"), ExpirationT: uint64(0)},
		{Key: []byte("hello5_12345678"), Value: []byte("world5"), ExpirationT: uint64(0)},
		{Key: []byte("hello6_12345678"), Value: []byte("world6"), ExpirationT: uint64(0)},
		{Key: []byte("hello7_12345678"), Value: []byte("world7"), ExpirationT: uint64(0)},
	}
	// 初始化opt
	opt = &Options{
		WorkerDir:          "../work_test",
		SSTableMaxSize:     283,
		MemTableSize:       224,
		BlockSize:          1024,
		BloomFalsePositive: 0.01,
	}
)


