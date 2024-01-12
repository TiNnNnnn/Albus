package lsm

import (
	"sync"
)

type levelManager struct {
	maxFid      uint64
	opt         *Options
	cache       *LsmCache
	maifestFile uint64
	levels      []*levelHandler
}

type levelHandler struct {
	sync.RWMutex
	levelNum int
	tables   []*table
}
