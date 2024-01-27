package utils

import (
	"encoding/binary"
	"time"
)

// skiplist存储的value信息（only for skiplist）
type ValueStruct struct {
	Value []byte
	//过期时间
	ExpirationT uint64
	Meta        byte
}

// 获取vs编码后长度
func (vs *ValueStruct) EncodeSize() uint32 {
	sz := len(vs.Value)
	ez := sizeOfUint64(vs.ExpirationT)
	return uint32(sz + ez + 1)
}

// 对value进行编码，并写入byte数组
func (vs *ValueStruct) EncodeValue(buf []byte) uint32 {
	buf[0] = vs.Meta
	sizeofExT := binary.PutUvarint(buf[1:], vs.ExpirationT)
	n := copy(buf[1+sizeofExT:], vs.Value)
	return uint32(sizeofExT + n + 1)
}

// 将编码的value和expiration进行解码
func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sizeofExT int
	vs.ExpirationT, sizeofExT = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sizeofExT:]

}

// 计算int64类型数据占用内存大小(可变长编码)
func sizeOfUint64(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// 用户写入的结构体
type Entry struct {
	Key         []byte
	Value       []byte
	ExpirationT uint64
	Meta        byte

	Version      uint64
	Offset       uint32
	Hlen         int
	ValThreshold int64
}

// 构建一个entry
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// 获取entry对象
func (e *Entry) Entry() *Entry {
	return e
}

// 根据ExpirtionT 判定entry是否合法
func (e *Entry) IsUnValid() bool {
	if e.Value == nil {
		return true
	}
	// 如果 ExpirationT 不为 0 且大于当前时间的 Unix 时间戳，认为条目无效
	if e.ExpirationT == 0 {
		return false
	}
	return e.ExpirationT <= uint64(time.Now().Unix())
}

// 设置entry有效时间
func (e *Entry) SetTLL(dt time.Duration) *Entry {
	e.ExpirationT = uint64(time.Now().Add(dt).Unix())
	return e
}

// 估算条目在内存中的大小
func (e *Entry) EstimateSize(threshold int) int {
	// TODO: 是否考虑 user meta?
	// 如果 Value 的长度小于阈值，直接返回键长、值长和 Meta 的总和
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 1 // Meta
	}
	// 如果 Value 的长度大于等于阈值，返回键长、12（ValuePointer）和 Meta 的总和
	return len(e.Key) + 12 + 1 // 12 for ValuePointer, 2 for meta.
}

// 获取entry序列化后长度（tips:只计算vlaue & expirationT）
func (e *Entry) EncodeSize() uint32 {
	valueSz := len(e.Value)
	enc := sizeOfUint64(e.ExpirationT)
	return uint32(valueSz + enc)
}

func (e *Entry) EncodeEntry(b []byte) uint32 {
	size := binary.PutUvarint(b[:], e.ExpirationT)
	valSz := copy(b[size:], e.Value)
	return uint32(size + valSz)
}

func (e *Entry) DecodeEntry(b []byte) {
	var size int
	e.ExpirationT, size = binary.Uvarint(b)
	e.Value = b[size:]
}
