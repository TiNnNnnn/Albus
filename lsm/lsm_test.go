package lsm

import (
	"albus/utils"
	"fmt"
	"log"
	"testing"
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

// 测试wal对lsm的恢复
func TestRecoveryBase(t *testing.T) {
	LSMSetTest()
	test := func() {
		lsm := NewLSM(opt)
		LSMGetTest(t, lsm)
	}
	runTest(test, 1)
}

func TestLSMBase(t *testing.T) {
	lsm := LSMSetTest()
	LSMGetTest(t, lsm)
}

func LSMSetTest() *LSM {
	lsm := NewLSM(opt)
	for _, entry := range entrys {
		lsm.Set(entry)
	}
	return lsm
}

func LSMGetTest(t *testing.T, lsm *LSM) {

	for i := 0; i <= 7; i++ {
		key := fmt.Sprintf("hello%d_12345678", i)
		entry, err := lsm.Get([]byte(key))
		if err != nil {
			log.Printf("lsm get entry failed ,err:%v", err)
			return
		}
		//assert.Nil(t, err)
		//assert.Equal(t, []byte("world2"), entry.Value)
		t.Logf("Get key=%s, value=%s,expiresAt=%d", entry.Key, entry.Value, entry.ExpirationT)
	}
}

func runTest(test func(), n int) {
	for i := 0; i < n; i++ {
		test()
	}
}
