package utils

import "math"

type Filter struct {
	table   []byte
	hashnum uint8
}

// 构建一个新bloomfilter
// args: 元素个数 ， false positive
func NewFilter(numEntries int, fp float64) *Filter {
	bpk := BitsPerKey(numEntries, fp)
	return initFilter(numEntries, bpk)
}

// 是否大概率存在
func (f *Filter) MayContainKey(key []byte) bool {
	return f.MayContain(Hash(key))
}

// 插入key
func (f *Filter) InsertKey(key []byte) bool {
	return f.Insert(Hash(key))
}

// 检查key是否存在，不存在则插入
func (f *Filter) AllowKey(key []byte) bool {
	if f == nil {
		return true
	}
	isExist := f.MayContainKey(key)
	if !isExist {
		f.InsertKey(key)
	}
	return isExist
}

// 重置BloomFilter
func (f *Filter) reset() {
	if f == nil {
		return
	}
	for i := range f.table {
		f.table[i] = 0
	}
}

func initFilter(numEntries int, bitsPerKey int) *Filter {
	newfilter := &Filter{}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	//计算哈希函数个数
	hashNum := uint32(float64(bitsPerKey) * 0.69)
	if hashNum < 1 {
		hashNum = 1
	}
	if hashNum > 30 {
		hashNum = 30
	}
	newfilter.hashnum = uint8(hashNum)

	//计算filter维数组大小,确保最低64b
	keyBits := numEntries * bitsPerKey
	if keyBits < 64 {
		keyBits = 64
	}
	keyBytes := (keyBits + 7) / 8
	//keyBits = keyBytes * 8

	newtbitmap := make([]byte, keyBytes+1)
	newtbitmap[keyBytes] = uint8(hashNum)

	newfilter.table = newtbitmap
	return newfilter

}

func (f *Filter) MayContain(h uint32) bool {
	if len(f.table) < 2 {
		return false
	}
	hashNum := f.table[len(f.table)-1]
	if hashNum > 30 {
		return true
	}
	nBits := uint32(8 * (len(f.table) - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < hashNum; j++ {
		bitPos := h % nBits
		if f.table[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func (f *Filter) Insert(h uint32) bool {
	hashNum := f.table[len(f.table)-1]
	if hashNum > 30 {
		return true
	}
	nBits := uint32(8 * (len(f.table) - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < hashNum; j++ {
		bitPos := h % nBits
		f.table[bitPos/8] |= 1 << (bitPos % 8)
		h += delta
	}
	return true
}

func (f *Filter) Allow(h uint32) bool {
	if f == nil {
		return true
	}
	isExist := f.MayContain(h)
	if !isExist {
		//不存在则插入key
		f.Insert(h)
	}
	return isExist

}

func (f *Filter) Len() int32 {
	return int32(len(f.table))
}

func newFilterByKeys(keys []uint32, bitsPerKey int) *Filter {
	return appendFilter(keys, bitsPerKey)
}

func appendFilter(keys []uint32, bitsPerKey int) *Filter {
	newfilter := &Filter{}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	//计算哈希函数个数
	hashNum := uint32(float64(bitsPerKey) * 0.69)
	if hashNum < 1 {
		hashNum = 1
	}
	if hashNum > 30 {
		hashNum = 30
	}
	newfilter.hashnum = uint8(hashNum)

	//计算filter维数组大小,确保最低64b
	keyBits := len(keys) * bitsPerKey
	if keyBits < 64 {
		keyBits = 64
	}
	keyBytes := (keyBits + 7) / 8
	keyBits = keyBytes * 8
	table := make([]byte, keyBytes+1)

	for _, h := range keys {
		//delta:增量值，用于改变哈希值 ，模拟多哈希函数
		delta := h>>17 | h<<15
		for j := uint32(0); j < hashNum; j++ {
			bitPos := h % uint32(keyBits)
			table[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}
	table[keyBytes] = uint8(hashNum)
	newfilter.table = table
	return newfilter
}

// 计算m/n
// num: 数据量 fp:false positive
func BitsPerKey(num int, fp float64) int {
	size := -1 * float64(num) * math.Log((fp)) / math.Pow(float64(0.6931471806), 2)
	tmp := math.Ceil(size / float64(num))
	return int(tmp)
}

// MurmurHash技术，通过将输入字节切片的每个4字节块转换为32位无符号整数，
// 并对这些整数进行累加和乘法混淆，最终得到一个哈希值
func Hash(b []byte) uint32 {
	const (
		//初始种子值
		seed = 0xbc9f1d34
		//乘法常数
		m = 0xc6a4a793
	)
	//初始化哈希值，使用初始种子值异或输入字节切片长度乘以乘法常数
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		//将每4个字节转换为一个32位无符号整数，累加到哈希值h中
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		//乘以乘法常数
		h *= m
		//异或，增加混淆
		h ^= h >> 16
	}
	//处理剩余字节，根据字节数进行处理
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		//fallthroght 执行下一个case
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
