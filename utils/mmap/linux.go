package mmap

import (
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/unix"
)

func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		//如果writable参数为true，则会设置页面的内存保护，以便可以写入这些页面
		mtype |= unix.PROT_WRITE
	}
	// 调用 unix.Mmap 函数进行内存映射
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// 在内存中重新映射页面。这可以用来替代munmap + mmap的组合
func mremap(data []byte, size int) ([]byte, error) {
	// 定义 mremap 的标志，MREMAP_MAYMOVE 表示映射可能会移动
	const MREMAP_MAYMOVE = 0x1

	// 将 data 转换为 reflect.SliceHeader，以获取底层的数据结构信息
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))

	// 调用 SYS_MREMAP 系统调用
	mmapAddr, _, errno := unix.Syscall6(
		unix.SYS_MREMAP,         // 系统调用编号
		header.Data,             // 原始映射地址
		uintptr(header.Len),     // 原始映射大小
		uintptr(size),           // 新的映射大小
		uintptr(MREMAP_MAYMOVE), // 标志，表示映射可能会移动
		0,                       // 新的映射地址，0 表示由系统选择新的地址
		0,                       // 附加的标志，这里设置为 0
	)

	// 如果系统调用返回错误，返回错误信息
	if errno != 0 {
		return nil, errno
	}

	// 更新 SliceHeader 的数据，容量和长度
	header.Data = mmapAddr
	header.Cap = size
	header.Len = size

	// 返回原始的数据切片
	return data, nil
}

func munmap(data []byte) error {
	if len(data) == 0 || len(data) != cap(data) {
		return unix.EINVAL
	}
	_, _, errno := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// 提供内存使用建议(user2os)
func madvise(b []byte, readhead bool) error {
	flags := unix.MADV_NORMAL
	if !readhead {
		//期望页面引用的顺序是随机的，可以将 readahead 标志设置为 false
		flags = unix.MADV_RANDOM
	}
	return unix.Madvise(b, flags)
}

// 将指定内存区域已修改数据希尔到磁盘
func msync(b []byte) error {
	//同步写入
	return unix.Msync(b, unix.MS_SYNC)
}
