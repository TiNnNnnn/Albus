package utils

import (
	"encoding/binary"
	"reflect"
	"unsafe"
)

// TODO: kv分离
type ValuePtr struct {
}

func NewvaluePtr(entry *Entry) *ValuePtr {
	return &ValuePtr{}
}

func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func U32ToBytes(n uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], n)
	return buf[:]
}

func U64ToBytes(n uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n)
	return buf[:]
}

// 将uint32切片转化为字节切片
func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	header.Len = len(u32s) * 4
	header.Cap = header.Len
	header.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	header := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	header.Len = len(b) / 4
	header.Cap = header.Len
	header.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}
