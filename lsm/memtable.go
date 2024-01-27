package lsm

import (
	"albus/file"
	"albus/utils"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

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

// 构建一个内存管理器
func (lsm *LSM) Newmemtable() *memTable {
	//fid for wal file
	fid := atomic.AddUint32(&lsm.maxMemFid, 1)
	//设置wal fileX相关参数
	fileOpt := &file.Options{
		Dir:  lsm.option.WorkerDir,
		Flag: os.O_CREATE | os.O_RDWR,
		//wal file的上限大小
		//TODO: wal file文件大小如何设置？
		MaxSize:  int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: memtableFilePath(lsm.option.WorkerDir, fid),
	}
	return &memTable{
		wal: file.OpenWalFile(fileOpt),
		//skiplist arena:1MB
		sl:  utils.NewSkipList(int64(1 << 20)),
		lsm: lsm,
	}
}

// eg:
// dir: /path/directory,fid:123,walFilExt = ./wal
// ret:/path/directory/00123.wal
func memtableFilePath(dir string, fid uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFilExt))
}

// 关闭wal文件
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
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
	walfileList, err := os.ReadDir(lsm.option.WorkerDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}
	//获取所有wal的fid
	var fidList []int
	for _, file := range walfileList {
		if !strings.HasSuffix(file.Name(), walFilExt) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseInt(file.Name()[:fsz-len(walFilExt)], 10, 64)
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fidList = append(fidList, int(fid))
	}
	//排序
	sort.Slice(fidList, func(i, j int) bool {
		return fidList[i] < fidList[j]
	})

	if len(fidList) != 0 {
		atomic.StoreUint32(&lsm.maxMemFid, uint32(fidList[len(fidList)-1]))
	}

	//恢复数据到immutables
	immutables := []*memTable{}
	for _, fid := range fidList {
		mt, err := lsm.openMemTable(uint32(fid))
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			continue
		}
		//TODO:最后一个skiplist不一定写满了,造成了空间的浪费
		immutables = append(immutables, mt)
	}
	return lsm.Newmemtable(), immutables
}

func (lsm *LSM) openMemTable(fid uint32) (*memTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkerDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: memtableFilePath(lsm.option.WorkerDir, fid),
	}
	s := utils.NewSkipList(int64(1 << 20))
	memtable := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	memtable.wal = file.OpenWalFile(fileOpt)
	//根据val files恢复memtable skiplist
	err := memtable.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "updating skiplist error"))
	return memtable, nil
}

// TODO
func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	endOff, err := m.wal.Iterator(true, 0, m.replay(m.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("iterator wal file error: %s", m.wal.Name()))
	}
	//log.Printf("iterator wal file success\n")
	return m.wal.Truncate(int64(endOff))
}

func (m *memTable) replay(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error {
		if ts := utils.ParseTimeFromKey(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.sl.Insert(e)
		return nil
	}
}

func (m *memTable) Size() int64 {
	return m.sl.MemSize()
}
