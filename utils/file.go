package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
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

// 同步目录项到磁盘,确保文件系统一致性
/*
通常情况下，写入文件系统的数据首先会被缓存在内存中，这被称为写缓存（Write Cache）
或脏缓存（Dirty Cache）。系统通过这种方式可以提高写入性能，因为将数据直接写入磁盘
的速度相对较慢，而将数据先写入内存，然后通过后续的机制将其刷写到磁盘，可以更高效地
完成写入操作
*/
func SyncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "open dir error: %s", dir)
	}
	err = f.Sync()
	cerr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "sync dir error: %s", dir)
	}
	return errors.Wrapf(cerr, "close directory: %s", dir)
}

func openDir(path string) (*os.File, error) {
	return os.Open(path)
}

// 负责读写文件管理
type BufReader struct {
	Reader *bufio.Reader
	Count  int64
}

// 重写了Reader的接口Reader，添加已读计数器
func (r *BufReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.Count += int64(n)
	return
}

// 收集 work dir 下所有文件id
func LoadIdMap(dir string) map[uint64]struct{} {
	filesInfo, err := os.ReadDir(dir)
	Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range filesInfo {
		if info.IsDir() {
			continue
		}
		fid := GetFidByPath(info.Name())
		if fid != 0 {
			idMap[fid] = struct{}{}
		}
	}
	return idMap
}
