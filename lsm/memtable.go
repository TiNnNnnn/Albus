package lsm

import (
	"albus/file"
	"albus/utils"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *utils.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

const walFilExt string = ".wal"

func (lsm *LSM) Newmemtable() *memTable {
	//fid := atomic.AddUint32(&lsm.maxMemFid, 1)
	//设置wal fileX相关参数
	// fileOpt := &file.Options{
	// 	Dir:  lsm.option.WorkerDir,
	// 	Flag: os.O_CREATE | os.O_RDWR,
	// 	//wal file的上限大小
	// 	MaxSize:  int(lsm.option.MemTableSize),
	// 	FID:      fid,
	// 	FileName: memtableFIlePath(lsm.option.WorkerDir, fid),
	// }
	return &memTable{
		//wal: file.OpenWalFile(fileOpt),
		wal: nil,
		//skiplist:1MB
		sl:  utils.NewSkipList(int64(1 << 20)),
		lsm: lsm,
	}
}

// eg:
// dir: /path/directory,fid:123,walFilExt = ./wal
// ret:/path/directory/00123.wal
func memtableFIlePath(dir string, fid uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFilExt))
}

// 关闭wal文件
func (m *memTable) close() error {
	// if err := m.wal.Close(); err != nil {
	// 	return err
	// }
	return nil
}

// 构建memtable
// 先写val,再写入内存
func (m *memTable) set(entry *utils.Entry) error {
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	m.sl.Insert(entry)
	log.Printf("insert entry [key: %s] to memtable success", entry.Key)
	return nil
}

// 从memtable中查询key
func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	vs := m.sl.Search(key)
	e := &utils.Entry{
		Key:         key,
		Value:       vs.Value,
		ExpirationT: vs.ExpirationT,
		Meta:        vs.Meta,
	}
	return e, nil
}

// wal文件恢复
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	return lsm.Newmemtable(), nil
}

func (lsm *LSM) openMemTable(fid uint32) (*memTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkerDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: memtableFIlePath(lsm.option.WorkerDir, fid),
	}
	s := utils.NewSkipList(int64(1 << 20))
	memtable := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	memtable.wal = file.OpenWalFile(fileOpt)
	err := memtable.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "updating skiplist error"))
	return memtable, nil
}

// TODO
func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	return nil
}

func (m *memTable) Size() int64 {
	return m.sl.MemSize()
}
