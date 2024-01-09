package utils

import "testing"

func (f *Filter) f2String() string {
	// 创建一个字节切片，用于存储表示位数组的字符串
	s := make([]byte, 8*len(f.table))

	// 遍历位数组的每个字节
	for i, x := range f.table {
		// 遍历当前字节的每个位
		for j := 0; j < 8; j++ {
			// 检查当前位是否为1，将对应的字符写入字节切片
			if x&(1<<uint(j)) != 0 {
				s[8*i+j] = '1'
			} else {
				s[8*i+j] = '.'
			}
		}
	}
	// 将字节切片转换为字符串并返回
	return string(s)
}

func TestSmallBloomFilter(t *testing.T) {
	var hash []uint32
	wordList := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}

	for _, word := range wordList {
		hash = append(hash, Hash(word))
	}
	f := newFilter(hash, 10)
	f2string := f.f2String()
	wantstring := "1...1.........1.........1.....1...1...1.....1.........1.....1....11....."
	if f2string != wantstring {
		t.Fatalf("f2string: %q\n wantstring: %q", f2string, wantstring)
	}

	m := map[string]bool{
		"hello": true,
		"world": true,
		"x":     false,
		"albus": false,
	}

	for k, isExist := range m {
		ans := f.MayContainKey([]byte(k))
		if ans != isExist {
			t.Fatalf("MayContain: key=%q\n f2string: %q\n wantstring: %q", k, f2string, wantstring)
		}
	}
}

func TestBloomFilter(t *testing.T) {
	nextLength := func(x int) int {
		if x < 10 {
			return x + 1
		}
		if x < 100 {
			return x + 10
		}
		if x < 1000 {
			return x + 100
		}
		return x + 1000
	}
	le32 := func(i int) []byte {
		b := make([]byte, 4)
		b[0] = uint8(uint32(i) >> 0)
		b[1] = uint8(uint32(i) >> 8)
		b[2] = uint8(uint32(i) >> 16)
		b[3] = uint8(uint32(i) >> 24)
		return b
	}

	nMediocreFilters, nGoodFilters := 0, 0
loop:
	for length := 1; length <= 10000; length = nextLength(length) {
		keys := make([][]byte, 0, length)
		for i := 0; i < length; i++ {
			keys = append(keys, le32(i))
		}
		var hashes []uint32
		for _, key := range keys {
			hashes = append(hashes, Hash(key))
		}
		f := newFilter(hashes, 10)

		if len(f.table) > (length*10/8)+40 {
			t.Errorf("length=%d: len(f)=%d is too large", length, len(f.table))
			continue
		}

		// All added keys must match.
		for _, key := range keys {
			if !f.MayContainKey(key) {
				t.Errorf("length=%d: did not contain key %q", length, key)
				continue loop
			}
		}

		// Check false positive rate.
		nFalsePositive := 0
		for i := 0; i < 10000; i++ {
			if f.MayContainKey(le32(1e9 + i)) {
				nFalsePositive++
			}
		}
		if nFalsePositive > 0.02*10000 {
			t.Errorf("length=%d: %d false positives in 10000", length, nFalsePositive)
			continue
		}
		if nFalsePositive > 0.0125*10000 {
			nMediocreFilters++
		} else {
			nGoodFilters++
		}
	}

	if nMediocreFilters > nGoodFilters/5 {
		t.Errorf("%d mediocre filters but only %d good filters", nMediocreFilters, nGoodFilters)
	}
}

func TestHash(t *testing.T) {
	testCases := []struct {
		s    string
		want uint32
	}{
		{"", 0xbc9f1d34},
		{"g", 0xd04a8bda},
		{"go", 0x3e0b0745},
		{"gop", 0x0c326610},
		{"goph", 0x8c9d6390},
		{"gophe", 0x9bfd4b0a},
		{"gopher", 0xa78edc7c},
		{"I had a dream it would end this way.", 0xe14a9db9},
	}
	for _, tc := range testCases {
		if got := Hash([]byte(tc.s)); got != tc.want {
			t.Errorf("s=%q: got 0x%08x, want 0x%08x", tc.s, got, tc.want)
		}
	}
}
