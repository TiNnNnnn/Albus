package file

import (
	"albus/utils"
	"bufio"
	"bytes"
	_ "errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
)

// wal 解决数据库进程崩溃造成的内存数据丢失问题
type WalFile struct {
	lock    *sync.RWMutex
	f       *MmapFile
	fid     uint32
	opts    *Options
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
}

// TODO:封装KV分离的读操作
type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	LF           *WalFile
}

func OpenWalFile(opt *Options) *WalFile {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSize)
	wf := &WalFile{
		f:    mf,
		lock: &sync.RWMutex{},
		opts: opt,
		buf:  &bytes.Buffer{},
		size: uint32(len(mf.Data)),
	}
	utils.Err(err)
	return wf
}

func (wf *WalFile) Close() error {
	//关闭wal文件
	if err := wf.f.Close(); err != nil {
		return err
	}
	//删除对于wal文件
	filename := wf.Name()
	os.Remove(filename)
	return nil
}

// 写wal文件
func (wf *WalFile) Write(entry *utils.Entry) error {
	wf.lock.Lock()
	defer wf.lock.Unlock()
	//序列化entry到buf
	len := utils.WalEncode(wf.buf, entry)
	//将序列化的数据拷贝到mmap区
	utils.CondPanic(wf.f.Appendbuffer(wf.writeAt, wf.buf.Bytes()) != nil,
		errors.New("wal writer error"))
	wf.writeAt += uint32(len)
	return nil
}

// 遍历磁盘中的walfile，获取数据
func (wf *WalFile) Iterator(readOnly bool, offset uint32, fn utils.LogEntry) (uint32, error) {
	//新建一个reader从给定的mmap reader读数据
	//ps: bufio.Reader的作用：将给定的io.Reader包装为具有缓冲功能
	//    能的读取器，优化读取性能，减少系统调用的次数
	//tip：即使在使用 reader 对象的过程中没有显式地进行读写操作，
	//	   bufio.Reader 也会在后台进行数据的缓存
	reader := bufio.NewReader(wf.f.NewReader(int(offset)))
	read := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: offset,
		LF:           wf,
	}
	var validEndOffset uint32 = offset
	//循环读取
loop:
	for {
		e, err := read.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case e.IsKeyZero():
			break loop
		case err != nil:
			return 0, err
		}

		//TODO kv分离
		var vptr utils.ValuePtr
		size := uint32(int(e.LogHeaderLen()) + len(e.Key) + len(e.Value) + crc32.Size)
		read.RecordOffset += size
		validEndOffset = read.RecordOffset

		if err := fn(e, &vptr); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function error")
		}
	}
	return validEndOffset, nil
}

func (wf *WalFile) Truncate(end int64) error {
	if end <= 0 {
		return nil
	}
	if fileinfo, err := wf.f.Fd.Stat(); err != nil {
		return fmt.Errorf("file %s has error with file.stat:%v", wf.Name(), err)
	} else if fileinfo.Size() == end {
		//大小正好相等，不需要截断
		return nil
	}
	wf.size = uint32(end)
	return wf.f.Truncature(end)
}

func (r *SafeRead) MakeEntry(reader io.Reader) (*utils.Entry, error) {
	rr := utils.NewHashReader(reader)
	var h utils.WalHeader
	hlen, err := h.DecodeWalHeader(rr)
	if err != nil {
		return nil, err
	}
	if h.KeyLen > uint32(1<<16) {
		return nil, utils.ErrTruncate
	}
	keylen := int(h.KeyLen)
	if cap(r.K) < keylen {
		r.K = make([]byte, 2*keylen)
	}
	vallen := int(h.ValueLen)
	if cap(r.V) < vallen {
		r.V = make([]byte, 2*vallen)
	}

	e := &utils.Entry{}
	e.Offset = r.RecordOffset
	e.Hlen = hlen
	e.ExpirationT = h.ExpirationT
	//读取key和value
	buf := make([]byte, h.KeyLen+h.ValueLen)
	if _, err := io.ReadFull(rr, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	e.Key = buf[:h.KeyLen]
	e.Value = buf[h.KeyLen:]

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.BytesToU32(crcBuf[:])
	if crc != rr.Sum32() {
		return nil, utils.ErrTruncate
	}
	return e, nil
}

func (wf *WalFile) Name() string {
	return wf.f.Fd.Name()
}

func (wf *WalFile) Size() uint32 {
	return wf.writeAt
}
