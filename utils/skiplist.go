package utils

import (
	"fmt"
	"log"
	"math"
	"strings"
	"sync/atomic"
	_ "unsafe"

	"github.com/pkg/errors"
)

const (
	MAXHEIGHT = 20
	GROWTHP   = math.MaxUint32 / 3
)

type SkipListNode struct {
	//value的指针
	//将value offset 与 value size 合并编码，方便实现CAS操作
	//低31位存放offset,高31位存放size
	// value offset: uint32(bits 0-31)
	// value size  : uint32(bits 32-63)
	value uint64

	//key的指针
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

// 创建一个新的跳表
func NewSkipList(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	head := NewNode(arena, nil, ValueStruct{}, MAXHEIGHT)
	headOffset := arena.getNodeOffset(head)
	return &SkipList{
		height:     1,
		arena:      arena,
		ref:        1,
		headOffset: headOffset,
	}
}

// 创建一个新的跳表节点
func NewNode(arena *Arena, key []byte, v ValueStruct, height int) *SkipListNode {
	nodeOffset := arena.addNode(height)
	keyOffset := arena.addKey(key)

	val := encodeValue(arena.addVal(v), v.EncodeSize())

	node := arena.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = val

	//log.Printf("new skiplist node success")
	return node
}

// 构建一个skiplist迭代器
func (s *SkipList) NewSkipListIterator() Iterator {
	s.IncRef()
	return &SkipListIterator{
		skiplist: s,
	}
}

// 引用计数加1
func (s *SkipList) IncRef() {
	atomic.AddInt32(&s.ref, 1)
}

// 引用计数减1，当减为0时释放资源
func (s *SkipList) DecRef() {
	ref := atomic.AddInt32(&s.ref, -1)
	if ref > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}
}

// 生成新节点随机高度
func (s *SkipList) getRandomHeight() int {
	h := 1
	for FastRand() <= GROWTHP && h < MAXHEIGHT {
		h++
	}
	return h
}

func (s *SkipList) Insert(e *Entry) {
	k, v := e.Key, ValueStruct{
		Value:       e.Value,
		ExpirationT: e.ExpirationT,
		Meta:        e.Meta,
	}
	height := s.getHeight()
	var prev [MAXHEIGHT + 1]uint32
	var next [MAXHEIGHT + 1]uint32

	prev[height] = s.headOffset
	for i := int(s.height) - 1; i >= 0; i-- {
		prev[i], next[i] = s.findposForLevel(k, prev[i+1], i)
		if prev[i] == next[i] {
			//key存在,更新value
			valueoffset := s.arena.addVal(v)
			encValue := encodeValue(valueoffset, v.EncodeSize())
			prevNode := s.arena.getNode(prev[i])
			prevNode.setValue(s.arena, encValue)
			return
		}
	}

	//key不存在，创建新节点
	newHeight := s.getRandomHeight()
	newNode := NewNode(s.arena, k, v, newHeight)

	//更新跳表高度
	listHeight := s.getHeight()
	for newHeight > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(newHeight)) {
			break
		}
		listHeight = s.getHeight()
	}

	for i := 0; i < newHeight; i++ {
		for {
			if s.arena.getNode(prev[i]) == nil {
				AssertTrue(i > 1)
				prev[i], next[i] = s.findposForLevel(k, s.headOffset, i)
				AssertTrue(prev[i] != next[i])
			}
			//插入新节点
			newNode.next[i] = next[i]
			prevNode := s.arena.getNode(prev[i])
			if prevNode.setNextOffset(i, next[i], s.arena.getNodeOffset(newNode)) {
				break
			}

			//cas失败，重新尝试
			prev[i], next[i] = s.findposForLevel(k, prev[i], i)
			//并发问题：此时新节点在skiplist已有值
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can only happen on base level:%d", i)
				valueoffset := s.arena.addVal(v)
				encValue := encodeValue(valueoffset, v.EncodeSize())
				prevNode := s.arena.getNode(prev[i])
				prevNode.setValue(s.arena, encValue)
				return
			}
		}
	}
}

// 搜索节点
func (s *SkipList) Search(key []byte) ValueStruct {
	//知道到node.key==key or node.key>key
	node, _ := s.findNear(key, false, true)
	if node == nil {
		return ValueStruct{}
	}
	nextKey := s.arena.getKey(node.keyOffset, node.keySize)
	if 0 != CompareKeys(key, nextKey) { //后期需要更改
		return ValueStruct{}
	}
	valueOffset, valueSize := node.getValueOffset()
	vs := s.arena.getVal(valueOffset, valueSize)
	return vs
}

// 找到一个节点level层上的pre和next
// args: key:目标key prev:前驱地址 level：当前高度
// return： 前驱地址，后驱节点
func (s *SkipList) findposForLevel(key []byte, prev uint32, level int) (uint32, uint32) {
	for {
		prevNode := s.arena.getNode(prev)
		next := prevNode.getNextOffset(level)
		nextNode := s.arena.getNode(next)
		if nextNode == nil {
			return prev, next
		}
		nextKey := nextNode.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 {
			return next, next
		} else if cmp < 0 {
			return prev, next
		} else {
			//向右走
			prev = next
		}
	}
}

// 知道指定节点的每一层前驱节点
func (s *SkipList) findPrevNode(key []byte) []uint32 {

	cur := s.getHead()

	h := s.getHeight() - 1
	prev := make([]uint32, h+1)

	for level := int(h); level >= 0; {
		nextNode := s.getNextNode(cur, level)
		cmp := CompareKeys(nextNode.key(s.arena), key)
		if nextNode != nil && cmp < 0 {
			cur = nextNode
		} else if nextNode == nil || cmp >= 0 {
			prev[level] = cur.keyOffset
			level--
		} else {

		}
	}
	return prev
}

func (s *SkipList) isEmpty() bool {
	return s.findLast() == nil
}

func (s *SkipList) findLast() *SkipListNode {
	cur := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		next := s.getNextNode(cur, level)
		if next != nil {
			cur = next
			continue
		}
		if level == 0 {
			if cur == s.getHead() {
				return nil
			}
			return cur
		}
		level--
	}

}

// 找到距离目标key最近节点
// args: less=true: 找满足node.key<key的最右节点
// args: less=false:找到满足node.key>key的的最左节点
// return: 最近节点，最近节点key值是否等于key
func (s *SkipList) findNear(key []byte, less bool, allowEqual bool) (*SkipListNode, bool) {
	cur := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		next := s.getNextNode(cur, level)
		if next == nil {
			if level > 0 {
				level--
				continue
			}
			//找到最低层，且到达表尾，less=false,找不到更大值
			if !less {
				return nil, false
			}
			//skipList为空
			if cur == s.getHead() {
				return nil, false
			}
			return cur, false
		}
		nextKey := next.key(s.arena)
		cmp := CompareKeys(key, nextKey) //之后需要修改
		if cmp > 0 {
			//curkey<nextkey<key,向右走
			cur = next
			continue
		} else if cmp == 0 {
			//curkey<nextkey=key
			if allowEqual {
				return next, true
			}
			if !less {
				return s.getNextNode(next, 0), false
			}
			if level > 0 {
				level--
				continue
			}
			if cur == s.getHead() {
				return nil, false
			}
			return cur, false

		} else {
			//curkey < key < nextkey
			if level > 0 {
				level--
				continue
			}
			if !less {
				return next, false
			}
			if cur == s.getHead() {
				return nil, false
			}
			return cur, false
		}
	}
}

// 获取跳表高度
func (s *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// 获取指定节点指定高度的下一个节点
func (s *SkipList) getNextNode(node *SkipListNode, height int) *SkipListNode {
	return s.arena.getNode(node.getNextOffset(height))
}

// 获取头结点地址
func (s *SkipList) getHead() *SkipListNode {
	return s.arena.getNode(s.headOffset)
}

// 获取节点value的地址
func (node *SkipListNode) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&node.value)
	return decodeValue(value)
}

// 设置节点value值
func (node *SkipListNode) setValue(arena *Arena, valueOffset uint64) {
	atomic.StoreUint64(&node.value, valueOffset)
}

// 获取节点key值
func (node *SkipListNode) key(arena *Arena) []byte {
	return arena.getKey(node.keyOffset, node.keySize)
}

// 返回当前节点在h层的next指针
func (node *SkipListNode) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&node.next[h])
}

// 更新节点next指针
func (node *SkipListNode) setNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&node.next[h], old, val)
}

func (node *SkipListNode) getValueSturct(arena *Arena) ValueStruct {
	valOffset, valSize := node.getValueOffset()
	return arena.getVal(valOffset, valSize)
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

// 将value offset 与 value size 合并编码，方便实现CAS操作
func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

// 将value offset 与 value size 合并编码解码
func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

// skiplist占内存大小
func (s *SkipList) MemSize() int64 {
	//fmt.Printf(" memtable skiplist size: %d",s.arena.Size())
	return s.arena.Size()
}

func (s *SkipList) Print(align bool) {
	reverseTree := make([][]string, s.getHeight())
	head := s.getHead()
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		next := head
		//遍历level层的所有节点
		for {
			var nodelist string
			next = s.getNextNode(next, level)
			if next != nil {
				key := next.key(s.arena)
				vs := next.getValueSturct(s.arena)
				nodelist = fmt.Sprintf("%s:%s", key, vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodelist)
		}
	}
	//对齐各层节点
	if align && s.getHeight() > 1 {
		baseLevel := reverseTree[0]
		for level := 1; level < int(s.getHeight()); level++ {
			pos := 0
			for _, node := range baseLevel {
				if pos == len(reverseTree[level]) {
					break
				}
				if node != reverseTree[level][pos] {
					newstr := fmt.Sprintf(strings.Repeat("-", len(node)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newstr
				}
				pos++
			}
		}
	}

	//打印
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, node := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s ", node)
			} else {
				fmt.Printf("%s->", node)
			}
		}
		fmt.Println()
	}
}

// AssertTruef is AssertTrue with extra info.
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

type SkipListIterator struct {
	skiplist *SkipList
	node     *SkipListNode
}

// 重定位
func (s *SkipListIterator) Rewind() {
	s.First()
}

// 向用户返回item
func (s *SkipListIterator) Item() Item {
	return &Entry{
		Key:         s.Key(),
		Value:       s.Value().Value,
		ExpirationT: s.Value().ExpirationT,
		Meta:        s.Value().Meta,
		//TODO: add version
	}
}

func (s *SkipListIterator) Key() []byte {
	return s.skiplist.arena.getKey(s.node.keyOffset, s.node.keySize)
}

func (s *SkipListIterator) Value() ValueStruct {
	valOffset, valSize := s.node.getValueOffset()
	return s.skiplist.arena.getVal(valOffset, valSize)
}

func (s *SkipListIterator) ValueUint64() uint64 {
	return s.node.value
}

func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.node = s.skiplist.getNextNode(s.node, 0)
}

func (s *SkipListIterator) Prev() {
	AssertTrue(s.Valid())
	s.node, _ = s.skiplist.findNear(s.Key(), true, false)
}

// 找到key>=target
func (s *SkipListIterator) Seek(target []byte) {
	s.node, _ = s.skiplist.findNear(target, false, true)
}

// 扎到key<=taget
func (s *SkipListIterator) SeekForLess(target []byte) {
	s.node, _ = s.skiplist.findNear(target, true, true)
}

func (s *SkipListIterator) First() {
	s.node = s.skiplist.getNextNode(s.skiplist.getHead(), 0)
}

func (s *SkipListIterator) End() {
	s.node = s.skiplist.findLast()
}

func (s *SkipListIterator) Close() error {
	s.skiplist.DecRef()
	return nil
}

func (s *SkipListIterator) Valid() bool {
	return s.node != nil
}

// UniIterator 是单向内存表迭代器,是对 Iterator 的轻量封装
type UniIterator struct {
	iter     *Iterator
	reversed bool
}
