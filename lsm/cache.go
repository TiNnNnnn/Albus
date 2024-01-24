package lsm

import "albus/utils/cache"

type LsmCache struct {
	// key:fid,value:table
	indexCache *cache.Cache
	//fid_blockoffset,value: block []byte
	blockCache *cache.Cache
}

type blockbuffer struct {
	b []byte
}

func newLsmCache(opt *Options) *LsmCache {
	indexSz := 10
	blockSz := 10
	return &LsmCache{
		indexCache: cache.NewCache(indexSz),
		blockCache: cache.NewCache(blockSz),
	}
}

func (c *LsmCache) close() error {
	return nil
}

func (c *LsmCache) setIndex(fid uint32, t *table) {
	c.indexCache.Set(fid, t)
}
