package cache

import (
	"albus/utils"
	"container/list"
	"sync"
)

type Cache struct {
	m         sync.RWMutex
	lru       *windowLRU
	slru      *segmentedLRU
	door      *utils.Filter
	c         *CmSketch
	t         int32
	threshold int32
	data      map[uint32]*list.Element
}

type Options struct {
	lruPct uint8
}

func NewCache(size int) *Cache {
	return nil
}
