package utils

import (
	"math"
	"sync/atomic"
)

const (
	MAXHEIGHT = 20
	GROWTHP   = math.MaxUint32 / 3
)

type SkipListNode struct {
	//将value offset 与 value size 合并编码，方便实现CAS操作
	//低31位存放offset,高31位存放size
	// value offset: uint32(bits 0-31)
	// value size  : uint32(bits 32-63)
	value uint64

	//尽量减少key的内存占用
	keyOffset uint32 //不可变
	keySize   uint16 //不可变

	//节点实际高度(指针个数)
	height uint16

	//skiplist节点的next指针数组，默认初始化为MAXHEIGHT
	next [MAXHEIGHT]uint32
}

type SkipList struct {
	height int32
	//头结点偏移量
	headOffset uint32
	//引用计数
	ref int32
	//每个跳表有一个内存池对象
	arena *Arena

	OnClose func()
}

//创建一个新的跳表
func newSkipList(arenaSize int64) *SkipList {
	arena := newAerna(arenaSize)
	head := newNode(arena, nil, ValueStruct{}, MAXHEIGHT)
	headOffset := arena.getNodeOffset(head)
	return &SkipList{
		height:     1,
		arena:      arena,
		ref:        1,
		headOffset: headOffset,
	}
}

//创建一个新的跳表节点
func newNode(arena *Arena, key []byte, v ValueStruct, height int) *SkipListNode {
	nodeOffset := arena.addNode(height)
	keyOffset := arena.addKey(key)

	val := encodeValue(arena.addVal(v), v.EncodeSize())

	node := arena.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = val
	return node
}

//生成新节点随机高度
func (s *SkipList) getRandomHeight() int {
	h := 1
	for FastRand() <= GROWTHP && h < MAXHEIGHT {
		h++
	}
	return h
}


//获取指定节点指定高度的下一个节点
func (s *SkipList) getNextNode(node *SkipListNode, height int) *SkipListNode {
	return s.arena.getNode(node.getNextOffset(height))
}

//获取头结点地址
func (s *SkipList) getHead() *SkipListNode {
	return s.arena.getNode(s.headOffset)
}

//获取节点value的地址
func (node *SkipListNode) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&node.value)
	return decodeValue(value)
}

//设置节点value值
func (node *SkipListNode) setValue(arena *Arena, valueOffset uint64) {
	atomic.StoreUint64(&node.value, valueOffset)
}

//获取节点key值
func (node *SkipListNode) key(arena *Arena) []byte {
	return arena.getKey(node.keyOffset, node.keySize)
}

//返回当前节点在h层的next指针
func (node *SkipListNode) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&node.next[h])
}

//更新节点next指针
func (node *SkipListNode) setNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&node.next[h], old, val)
}

func (node *SkipListNode) getValueSturct(arena *Arena) ValueStruct {
	valOffset, valSize := node.getValueOffset()
	return arena.getVal(valOffset, valSize)
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

//将value offset 与 value size 合并编码，方便实现CAS操作
func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

//将value offset 与 value size 合并编码解码
func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}
