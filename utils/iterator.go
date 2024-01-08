package utils

/*
迭代器接口类
*/

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
