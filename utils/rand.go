package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

func RandInt63n(n int64) int64 {
	mu.Lock()
	res := r.Int63n(n)
	mu.Unlock()
	return res
}

func RandInt(n int) int {
	mu.Lock()
	res := r.Intn(n)
	mu.Unlock()
	return res
}

func RandFloat64() float64 {
	mu.Lock()
	res := r.Float64()
	mu.Unlock()
	return res
}

// 生成随机的key,value
func RandStr(length int) string {
	// 包括特殊字符,进行测试
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|©®😁😭🉑️🐂㎡硬核课堂"
	bytes := []byte(str)
	result := []byte{}
	source := rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(100)))
	r = rand.New(source)
	//rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

func BuildEntry() *Entry {
	source := rand.NewSource(time.Now().UnixNano())
	r = rand.New(source)
	key := []byte(fmt.Sprintf("%s%s", RandStr(16), "12345678"))
	value := []byte(RandStr(128))

	expirationT := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &Entry{
		Key:         key,
		Value:       value,
		ExpirationT: expirationT,
	}
}
