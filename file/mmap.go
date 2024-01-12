package file

import "os"

type MmapFile struct {
	Data []byte
	Fd   *os.File
}


