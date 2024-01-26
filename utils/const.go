package utils

import (
	"hash/crc32"
	"os"
)

const (
	MaxLevelNum           = 7
	DefaultValueThreshold = 1024
)

const (
	DefaultFileFlag = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode = 0666

	ManifestFilename            = "MANIFEST"
	ManifestRewriteFilename     = "REWRITEMANIFEST"
	ManifestDelReWriteThreshold = 10000
	ManifestDelRatio            = 10
)

var (
	//crc32.Maketable:创建一个CRC32校验码表
	//args:crc32.Castagnoli:预定义的CRC32算法，使用了Castagnoli多项式
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

	MagicText    = [4]byte{'T', 'I', 'N', 'N'}
	MagicVersion = uint32(1)
)
