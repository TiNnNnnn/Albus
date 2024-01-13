package cache

import (
	"albus/utils"

	"github.com/cespare/xxhash/v2"
)

func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return utils.MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return utils.MemHash(k), xxhash.Sum64(k)
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("key type isn't supposed")
	}
}
