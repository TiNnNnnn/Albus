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

//创建一个新的Aerna对象
func newAerna(n int64) *Arena {
	ret := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return ret
}

//从arena中申请内存空间
//args：sz: 需要的内存大小
//return: 申请的内存块的起始地址
func (s *Arena) allocate(sz uint32) uint32 {

	offset := atomic.AddUint32(&s.n, sz)

	//当剩下的内存空间已经不足以放下一个新节点,则进行扩容
	if len(s.buf)-int(offset) < MAXNODESIZE {
		grow := uint32(len(s.buf))
		if grow > 1<<30 {
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

//向arena申请一个skiplistnode大小的空间
//args: skiplistnode节点的实际高度
//return: 申请的空间的起始地址（内存对齐之后的值）
func (s *Arena) addNode(height int) uint32 {
	len := MAXNODESIZE - ((MAXNODESIZE - height) * OFFSETSIZE) + NODEALIGN
	n := s.allocate(uint32(len))
	alignN := (n + uint32(NODEALIGN)) & ^uint32(NODEALIGN)
	return alignN
}

func (s *Arena) addVal(v ValueStruct) uint32 {
	l := uint32(v.EncodeSize())
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) addKey(key []byte) uint32 {
	l := uint32(len(key))
	offset := s.allocate(l)
	buf := s.buf[offset : offset+l]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}


func (s *Arena) getNode(offset uint32) *SkipListNode {
	if offset == 0 {
		return nil
	}
	return (*SkipListNode)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getNodeOffset(node *SkipListNode) uint32 {
	if node == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (retVs ValueStruct) {
	retVs.DncodeValue(s.buf[offset : offset+size])
	return
}

//获取aerna已分配空间
func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

//判断拷贝是否出现问题
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.New("Assert failed"))
	}
}
