package utils

import (
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
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

func CompareKeys(k1, k2 []byte) int {
	return 0
}
