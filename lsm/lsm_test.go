package lsm

import (
	"albus/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

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



func BaseTest(t *testing.T, lsm *LSM) {
	entry, err := lsm.Get([]byte("hello7_12345678"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world7"), entry.Value)
	t.Logf("Get key=%s, value=%s,expiresAt=%d", entry.Key, entry.Value, entry.ExpirationT)
}
