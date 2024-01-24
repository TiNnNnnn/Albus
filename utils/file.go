package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// 通过文件路径获取文件id
func GetFidByPath(name string) uint64 {
	//get filename by file path
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		Err(err)
		return 0
	}
	return uint64(id)
}

// 生成sst文件名
func GenSSTPath(dir string, id uint64) string {
	name := fmt.Sprintf("%06d.sst", id)
	return filepath.Join(dir, name)
}

// 比较key
// 先比较key的数值内容，数值内容相等则比较时间戳
func CompareKeys(k1, k2 []byte) int {
	CondPanic((len(k1) <= 8 || len(k2) <= 8), fmt.Errorf("%s,%s less than 8", k1, k2))
	if cmp := bytes.Compare(k1[:len(k1)-8], k2[:len(k2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(k1[len(k1)-8:], k2[len(k2)-8:])
}

// 计算数据的CRC32校验和
func CalCheckSum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}

// 对比校验和
func CompareCheckSum(data []byte, expected []byte) error {
	//log.Printf("exptected: %s", expected)
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual:%d,expected:%d", actual, expectedU64)
	}
	return nil
}
