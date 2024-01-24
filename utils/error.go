package utils

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	//gopath
	gopath = path.Join(os.Getenv("GOPATH"), "src") + "/"
)

// NotFoundKey 找不到key
var (
	ErrBadBuilderPushBack = errors.New("tableBuilder pushback error")
	ErrOutofInterger      = errors.New("Interger overflow")
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")
	// ErrReWriteFailure reWrite failure
	ErrReWriteFailure = errors.New("reWrite failure")
	// ErrBadMagic bad magic
	ErrBadMagic = errors.New("bad magic")
	// ErrBadChecksum bad check sum
	ErrBadChecksum = errors.New("bad check sum")
	// ErrChecksumMismatch is returned at checksum mismatch.
	ErrChecksumMismatch = errors.New("checksum mismatch")
)

// 错误日志格式化显示
func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}

// 获取调用函数位置信息
// args: deep:调用栈深度(从调用函数向上找deep层调用栈) fullPath:是否是完整路径
// rets: 返回 [filename:lineId]
func location(deep int, fullPath bool) string {
	//runtime.Caller:获取调用栈信息，包括文件名，行号...
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}
	//获取文件名
	if fullPath {
		//判断文件名是否以gopath开头
		if strings.HasPrefix(file, gopath) {
			file = file[len(gopath):]
		}
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}

// 断言assert,满足condition则panic
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}

// Panic 如果err 不为nil 则panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
