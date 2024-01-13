package file

import (
	"albus/utils"
	"albus/utils/mmap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type MmapFile struct {
	Data []byte
	Fd   *os.File
}

// 开辟sz大小的内存映射空间
func OpenMmapfileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fileinfo, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "can not get file stat:%s", filename)
	}

	var rerr error
	fileSize := fileinfo.Size()
	if sz > 0 && fileSize == 0 {
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		fileSize = int64(sz)
	}

	buf, err := mmap.Mmap(fd, writable, fileSize)
	if err != nil {
		return nil, errors.Wrapf(err, "mmap for %s failed,size:%d", fd.Name(), fileSize)
	}
	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go SyncDir(dir)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, rerr
}

// 同步目录更改到磁盘
func SyncDir(dir string) error {
	//打开目录
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	//同步目录
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	//关闭目录
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}

func OpenMmapFile(filename string, flag, maxSz int) (*MmapFile, error) {
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open:%s", filename)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	return OpenMmapfileUsing(fd, maxSz, writable)
}

type mmapReader struct {
	Data   []byte
	offset int
}

func (mr *mmapReader) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   mr.Data,
		offset: offset,
	}
}

func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

func (m *MmapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > len(m.Data) {
		return []byte{}
	}
	res := m.Data[start:next]
	return res
}

func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4
	if start+sz > len(m.Data) {
		growBy := len(m.Data)
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
		if err := m.Truncature(int64(len(m.Data) + growBy)); err != nil {
			return nil, 0, err
		}
	}
	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))
	return m.Data[start : start+sz], start + sz, nil
}

func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("munmap file:%s error:%v", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("truncate file:%s error:%v", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("close file:%s error:%v", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}

func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("sync file:%s error:%v", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

func (m *MmapFile) Truncature(maxSz int64) error {
	var err error
	if maxSz >= 0 {
		if err = m.Fd.Truncate(maxSz); err != nil {
			return fmt.Errorf("truncate file:%s error:%v", m.Fd.Name(), err)
		}
		if maxSz > int64(len(m.Data)) {
			m.Data, err = mmap.Mremap(m.Data, int(maxSz))
			//TODO
			return utils.Err(err)
		}
	}
	return nil
}
