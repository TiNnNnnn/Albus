package cache

import (
	"container/list"
	"fmt"
)

const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

type segmentedLRU struct {
	data               map[uint64]*list.Element
	stOneCap, stTwoCap int
	stageOne, stageTwo *list.List
}

func newSLRU(data map[uint64]*list.Element, sCap1, sCap2 int) *segmentedLRU {
	return &segmentedLRU{
		data:     data,
		stOneCap: sCap1,
		stTwoCap: sCap2,
		stageOne: list.New(),
		stageTwo: list.New(),
	}
}

func (slru *segmentedLRU) add(newitem storeItem) {
	newitem.stage = 1

	//stageOne没满，整个slru也没满
	if slru.stageOne.Len() < slru.stOneCap || slru.TotalLen() < slru.stOneCap+slru.stTwoCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
	}

	backelement := slru.stageOne.Back()
	item := backelement.Value.(*storeItem)
	delete(slru.data, item.key)

	*item = newitem
	slru.data[item.key] = backelement
	slru.stageOne.MoveToFront(backelement)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	//数据在 stage2,更新stage2即可
	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	//数据在 stage1,再次被访问，提升至stage2
	if slru.stageTwo.Len() < slru.stTwoCap {
		slru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	//数据在 stage1,再次被访问，提升至stage2，但是stage2已满，淘汰降级其旧数据进入stage1
	backelement := slru.stageTwo.Back()
	backitem := backelement.Value.(*storeItem)

	*backitem, *item = *item, *backitem

	backitem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	slru.data[item.key] = v
	slru.data[backitem.key] = backelement

	slru.stageOne.MoveToFront(v)
	slru.stageOne.MoveToFront(backelement)

}

func (slru *segmentedLRU) victim() *storeItem {
	if slru.TotalLen() <= slru.stOneCap+slru.stTwoCap {
		//slru未满，无需淘汰
		return nil
	} else {
		//slru已满,从stage1中淘汰数据
		backelement := slru.stageOne.Back()
		return backelement.Value.(*storeItem)
	}

}

func (slru *segmentedLRU) TotalLen() int {
	return slru.stageOne.Len() + slru.stageTwo.Len()
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v ", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf(" | ")
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v ", e.Value.(*storeItem).value)
	}
	return s
}
