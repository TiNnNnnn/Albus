package utils

import "math"

type Filter struct {
	table []byte
}

func newFilter(keys []uint32, bitsPerKey int) *Filter {
	return &Filter{
		table: appendFilter(keys, bitsPerKey),
	}
}

func (f *Filter) MayContainKey(key []byte) bool {
	return f.MayContain(Hash(key))
}

func (f *Filter) MayContain(h uint32) bool {
	if len(f.table) < 2 {
		return false
	}
	k := f.table[len(f.table)-1]
	if k > 30 {
		return true
	}
	nBits := uint32(8 * (len(f.table) - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if f.table[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
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
	keyBits := len(keys) * bitsPerKey
	if keyBits < 64 {
		keyBits = 64
	}
	keyBytes := (keyBits + 7) / 8
	keyBits = keyBytes * 8
	filter := &Filter{
		table: make([]byte, keyBytes+1),
	}

	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < hashNum; j++ {
			bitPos := h % uint32(keyBits)
			filter.table[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}
	filter.table[keyBytes] = uint8(hashNum)
	return filter.table
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
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h ^= h >> 24
	}
	return h
}
