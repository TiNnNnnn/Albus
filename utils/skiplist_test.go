package utils

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		num := r.Intn(26) + 65 //A~Z
		bytes[i] = byte(num)
	}
	return string(bytes)
}

func TestSkipListCRUD(t *testing.T) {
	list := NewSkipList(1000)
	entry1 := NewEntry([]byte(RandString(10)), []byte("Val1"))
	list.Insert(entry1)
	vs := list.Search(entry1.Key)
	//fmt.Printf("vs: %v\n", vs)
	assert.Equal(t, entry1.Value, vs.Value)

	entry2 := NewEntry([]byte(RandString(10)), []byte("Val2"))
	list.Insert(entry2)
	vs = list.Search(entry2.Key)
	assert.Equal(t, entry2.Value, vs.Value)

	assert.Nil(t, list.Search([]byte(RandString(10))).Value)

	newEntry2 := NewEntry(entry1.Key, []byte("Val1+new"))
	list.Insert(newEntry2)
	assert.Equal(t, newEntry2.Value, list.Search(newEntry2.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList(100000000)
	key, val := "", ""
	maxTime := 1000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = RandString(10), fmt.Sprintf("Val%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		list.Insert(entry)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList(100000000)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("Keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Insert(NewEntry(key(i), key(i)))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			require.EqualValues(t, key(i), v.Value)
			return
			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList(100000000)
	//创建一个等待组（WaitGroup），等待并发操作完成。
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("keykeykey%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Insert(NewEntry(key(i), key(i)))
		}(i)
	}
	//等待各协程插入结束
	wg.Wait()

	//检查value,并发读
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			require.EqualValues(b, key(i), v.Value)
			require.NotNil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkipList(100000)

	//Put & Get
	entry1 := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Insert(entry1)
	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)

	entry2 := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Insert(entry2)
	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)

	//Update a entry
	entry2_new := NewEntry([]byte(RandString(10)), []byte(RandString(10)))
	list.Insert(entry2_new)
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)

	iter := list.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		fmt.Printf("iter key %s, value %s", iter.Item().Entry().Key, iter.Item().Entry().Value)
	}
}

func TestDrawList(t *testing.T) {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
	list := NewSkipList(1000)
	n := 12
	for i := 0; i < n; i++ {
		index := strconv.Itoa(r.Intn(90) + 10)
		key := index + RandString(8)
		entryRand := NewEntry([]byte(key), []byte(index))
		list.Insert(entryRand)
	}
	list.Print(true)
	fmt.Println(strings.Repeat("*", 30) + "分割线" + strings.Repeat("*", 30))
	list.Print(false)
}
