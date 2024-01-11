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
	cnt       int32
	threshold int32
	data      map[uint64]*list.Element
}

type Options struct {
	lruPct uint8
}

func NewCache(size int) *Cache {
	//windowlru缓存占比，默认1%
	const lruPct = 1
	lruSize := (lruPct * size) / 100
	if lruSize < 1 {
		lruSize = 1
	}
	//slru:80%
	slruSize := int(float64(size) * ((100 - lruPct) / 100.0))
	if slruSize < 1 {
		slruSize = 1
	}
	//slru1 = slru *20%
	slru1 := int(0.2 * float64(slruSize))
	if slru1 < 1 {
		slru1 = 1
	}

	data := make(map[uint64]*list.Element, size)
	return &Cache{
		lru:  newWindowLRU(lruSize, data),
		slru: newSLRU(data, slru1, slruSize-slru1),
		//允许1%的false postive
		door: utils.NewFilter(size, 0.01),
		c:    newCmSketch(int64(size)),
		data: data,
	}
}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Lock()
	return c.del(key)
}

func (c *Cache) set(key, value interface{}) bool {

	keyHash, conflictHash := c.keyToHash(key)

	item := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	eitem, evicted := c.lru.add(item)
	if !evicted {
		//wlru没有满，不需要淘汰元素
		return true
	} else {
		victim := c.slru.victim()
		if victim == nil {
			//slru没有满
			c.slru.add(eitem)
			return true
		} else {
			//slru已满
			//从slfu(stage1)中找到一个淘汰者，与windowlru的淘汰者 pk

			//过滤掉wlru中只被访问一次的淘汰者（pk必然失败）
			if !c.door.Allow(uint32(eitem.key)) {
				return true
			}

			cnt1 := c.c.Estimate(victim.key)
			cnt2 := c.c.Estimate(eitem.key)
			if cnt1 > cnt2 {
				return true
			}

			c.slru.add(eitem)
			return true
		}
	}
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.cnt++
	if c.cnt == c.threshold {
		//到达访问阈值，进行重置，减半旧数据频次
		c.c.Reset()
		c.door.Reset()
		c.cnt = 0
	}
	keyHash, confictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)

	//排除hashkey冲突可能
	if item.conflict != confictHash {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}

	c.door.Allow(uint32(keyHash))
	c.c.Increment(item.key)

	v := item.value

	if item.stage == 0 {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}
	return v, true
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}
	item := val.Value.(*storeItem)
	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}
	delete(c.data, keyHash)
	return item.conflict, true
}
