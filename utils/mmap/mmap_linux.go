package mmap

import "os"

func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

func Mremap(data []byte, size int) ([]byte, error) {
	return mremap(data, size)
}

func Munmap(b []byte) error {
	return munmap(b)
}

func Madvise(b []byte, readhead bool) error {
	return madvise(b, readhead)
}

func Msync(b []byte) error {
	return msync(b)
}
