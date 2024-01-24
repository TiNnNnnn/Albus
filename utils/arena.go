package utils

import (
	"errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	OFFSETSIZE  = int(unsafe.Sizeof(uint32(0)))
	MAXNODESIZE = int(unsafe.Sizeof(SkipListNode{}))
	//内存对齐
	NODEALIGN = int(unsafe.Sizeof(uint64(0))) - 1
)

type Arena struct {
	n   uint32 //Aerna已经分配的出去的内存大小
	buf []byte //Aerna预申请的内存空间
}

// 创建一个新的Aerna对象
func newArena(n int64) *Arena {
	newarena := &Arena{
		// 设定n=0时，为空指针
		n:   1,
		buf: make([]byte, n),
	}
	//log.Printf("create new aerna success, size: %d", n)
	return newarena
}

// 从arena中申请内存空间
// args：sz: 需要的内存大小
// return: 申请的内存块的起始地址
func (s *Arena) allocate(sz uint32) uint32 {

	offset := atomic.AddUint32(&s.n, sz)

	//当剩下的内存空间已经不足以放下一个新节点,则进行扩容
	if len(s.buf)-int(offset) < MAXNODESIZE {
		grow := uint32(len(s.buf))
		if grow > 1<<30 {
			//一次性最大扩容1GB
			grow = 1 << 30
		} else if grow < sz {
			grow = sz
		}
		newBuf := make([]byte, len(s.buf)+int(grow))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - sz
}

// 向arena申请一个skiplistnode大小的空间
// args: skiplistnode节点的实际高度
// return: 申请的空间的起始地址（内存对齐之后的值）
func (s *Arena) addNode(height int) uint32 {
	len := MAXNODESIZE - ((MAXHEIGHT - height) * OFFSETSIZE) + NODEALIGN
	n := s.allocate(uint32(len))
	alignN := (n + uint32(NODEALIGN)) & ^uint32(NODEALIGN)
	return alignN
}

// 申请一个vs大小的空间，并返回vs地址
func (s *Arena) addVal(v ValueStruct) uint32 {
	l := uint32(v.EncodeSize())
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

// 申请一个key大小的空间，并返回key地址
func (s *Arena) addKey(key []byte) uint32 {
	l := uint32(len(key))
	offset := s.allocate(l)
	buf := s.buf[offset : offset+l]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

// 获取skipList节点对象
// args: 偏移量
// return ：skiplist节点对象
func (s *Arena) getNode(offset uint32) *SkipListNode {
	if offset == 0 {
		return nil
	}
	return (*SkipListNode)(unsafe.Pointer(&s.buf[offset]))
}

// 获取skiplist节点地址
// args: skiplist节点对象
// return: skiplist节点地址
func (s *Arena) getNodeOffset(node *SkipListNode) uint32 {
	if node == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

// 获取key
// args: offset:key地址 size:key长度
// return: key
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

// 获取vs对象
// args: offset: vs地址 size:vs长度
// return: vs对象
func (s *Arena) getVal(offset uint32, size uint32) (retVs ValueStruct) {
	retVs.DecodeValue(s.buf[offset : offset+size])
	return
}

// 获取aerna已分配空间
func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

// 判断拷贝是否出现问题
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.New("Assert failed"))
	}
}
