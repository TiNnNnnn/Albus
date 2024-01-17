package lsm

import "albus/utils"

type Iterator struct {
	it    Item
	iters []utils.Iterator
}

type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}


