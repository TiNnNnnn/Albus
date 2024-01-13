package utils

import (
	"encoding/binary"
	"math"
	"unsafe"
)

type stringStruct struct {
	str unsafe.Pointer
	len int
}

// runtime.memhash用于计算给定内存块的哈希值
//args: 起始地址 种子值 内存长度
//return: 哈希值(uint64)

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// 解析出不带时间戳的Key
func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		return key
	}
	//[0,len(key)-8]
	return key[:len(key)-8]
}

// 解析出key的时间戳
func ParseTimeFromKey(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}

// 对字节数组进行哈希计算
func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// 对字符串进行哈希计算
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}
