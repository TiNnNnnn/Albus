package file

import (
	"albus/pb"
	"albus/utils"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// Manifest:存储sst所属层级关系的元数据文件，数据库重启时
// 需要读取该文件用于恢复层级关系
// 更新时机：每次flush 和 merge sst
type ManifestFile struct {
	opt *Options
	f   *os.File
	//文件锁，manifest全局唯一，写时加锁
	lock sync.Mutex
	//覆写发生的阈值
	deletionsRewriterThreshold int
	//manifest状态机
	mainfest *Manifest
}

type Manifest struct {
	//保存每一层的对应sst (每层一个map)
	Levels []levelManifest
	//用于快速查询一个table位于那一层
	Tables map[uint64]TableManifest
	//统计sst创建次数
	Creations int
	//统计sst删除次数
	Deletions int
}

// sstable的元数据
type TableManifest struct {
	Level    uint8
	checksum []byte
}

type levelManifest struct {
	Tables map[uint64]struct{}
}

// sst 的元信息
type TableMeta struct {
	Id       uint64
	CheckSum []byte
}

// 打开Manifest文件
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{
		lock: sync.Mutex{},
		opt:  opt,
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return mf, err
		}
		log.Printf("manifest not exist,create new manifest op\n")
		//manifast文件不存在,通过覆写方式创建manifast文件，将当前状态
		//抽象为changes追加为changset并序列化到文件中
		m := newManifest()
		fp, netCreations, err := helpRewrite(opt.Dir, m)
		utils.CondPanic(netCreations != 0, utils.ErrReWriteFailure)
		if err != nil {
			return mf, err
		}
		mf.f = fp
		f = fp
		mf.mainfest = m
		return mf, nil
	}
	log.Printf("manifest has exist,begin replay manifest op\n")
	//manifast存在，且打开成功,进行操作重放
	manifest, truncOffset, err := ReplayManifestFile(f)
	if err != nil {
		log.Printf("ReplayManifestFile error\n")
		_ = f.Close()
		return mf, err
	}
	//将文件截断为truncoffset,取出多余部分，防止出现空位置
	if err := f.Truncate(truncOffset); err != nil {
		log.Printf("Truncate manifest error\n")
		_ = f.Close()
		return mf, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		log.Printf("seek manifest error\n")
		_ = f.Close()
		return mf, err
	}
	mf.f = f
	mf.mainfest = manifest
	return mf, nil
}

// 构建manifest状态机
func newManifest() *Manifest {
	return &Manifest{
		Levels: make([]levelManifest, 0),
		Tables: make(map[uint64]TableManifest),
	}
}

// 对manifest文件重新应用所有状态变更
func ReplayManifestFile(fp *os.File) (ret *Manifest, truncOffset int64, err error) {
	r := &utils.BufReader{
		Reader: bufio.NewReader(fp),
	}
	//验证magic正确性
	var magicbuf [8]byte
	if _, err := io.ReadFull(r, magicbuf[:]); err != nil {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(magicbuf[0:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	magicVersion := binary.BigEndian.Uint32(magicbuf[4:8])
	if magicVersion != uint32(utils.MagicVersion) {
		return &Manifest{}, 0, fmt.Errorf("unsupported manofest verison:%d", magicVersion)
	}
	build := newManifest()

	//循环解析changset
	var offset int64
	for {
		offset = r.Count
		var lenAndCrcBuf [8]byte
		_, err := io.ReadFull(r, lenAndCrcBuf[:])
		if err != nil {
			//已经读写完毕
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		l := binary.BigEndian.Uint32(lenAndCrcBuf[0:4])
		var buf = make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) !=
			binary.BigEndian.Uint32(lenAndCrcBuf[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := proto.Unmarshal(buf, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
		if err := applyChangeSet(build, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	}
	return build, offset, err
}

func applyChangeSet(build *Manifest, changset *pb.ManifestChangeSet) error {
	for _, change := range changset.Changes {
		if err := applyChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

func applyChange(build *Manifest, tc *pb.ManifestChange) error {
	switch tc.Op {
	case pb.ManifestChange_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("MANIFAST invalid,able %d has exists", tc.Id)
		}
		//sst_id --> tablemanifast(sst_level,sst_checksum)
		build.Tables[tc.Id] = TableManifest{
			Level:    uint8(tc.Level),
			checksum: append([]byte{}, tc.Checksum...),
		}
		//levels
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, levelManifest{
				make(map[uint64]struct{}),
			})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case pb.ManifestChange_DELETE:
		tm, ok := build.Tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		delete(build.Levels[tm.Level].Tables, tc.Id)
		delete(build.Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange operate")
	}
	return nil
}

func (mf *ManifestFile) rewrite() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	fp, nextCreations, err := helpRewrite(mf.opt.Dir, mf.mainfest)
	if err != nil {
		return err
	}
	mf.mainfest.Creations = nextCreations
	mf.mainfest.Deletions = 0
	mf.f = fp
	return nil
}

// MANIFEST/REMANIFEST FORMAT: |Magic|changes|changes|.....
// changes:|len|crc|ManifestChangeSet|
func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	fp, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		log.Printf("open manifest rewritefile error\n")
		return nil, 0, err
	}
	//写magic
	// |Magic|
	buf := make([]byte, 8)
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion))

	netCreations := len(m.Tables)
	//log.Printf("net creations: %d\n", len(m.Tables))
	changes := m.getChanges()
	set := &pb.ManifestChangeSet{
		Changes: changes,
	}

	changeBuf, err := proto.Marshal(set)
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	//|Magic|len|crc|ManifestChangeSet|...
	var lenAndCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenAndCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenAndCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	buf = append(buf, lenAndCrcBuf[:]...)
	buf = append(buf, changeBuf...)

	if _, err := fp.Write(buf); err != nil {
		log.Printf("write buf to manifest file error")
		fp.Close()
		return nil, 0, err
	}
	//手动刷新文件新内容到磁盘
	if err := fp.Sync(); err != nil {
		log.Printf("sync manifest file new contents to disk error")
		fp.Close()
		return nil, 0, err
	}
	//文件重命名前先关闭文件
	if err := fp.Close(); err != nil {
		return nil, 0, err
	}
	//重命名
	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		log.Printf("REWRITEMANIFEST rename to MANIFEST error")
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := utils.SyncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}
	return fp, netCreations, nil
}

// 从tables获取所有sst changes
func (m *Manifest) getChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newManifestChange(id, int(tm.Level), tm.checksum))
	}
	return changes
}

func newManifestChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// 添加table元信息到manifest
func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	mf.addChanges([]*pb.ManifestChange{
		newManifestChange(t.Id, levelNum, t.CheckSum),
	})
	return err
}

func (mf *ManifestFile) addChanges(changeList []*pb.ManifestChange) error {
	changes := pb.ManifestChangeSet{
		Changes: changeList,
	}
	buf, err := proto.Marshal(&changes)
	if err != nil {
		return err
	}

	mf.lock.Lock()
	defer mf.lock.Unlock()
	if err := applyChangeSet(mf.mainfest, &changes); err != nil {
		return err
	}
	if mf.mainfest.Deletions > utils.ManifestDelReWriteThreshold &&
		mf.mainfest.Deletions > utils.ManifestDelRatio*(mf.mainfest.Creations-mf.mainfest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenAndCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenAndCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenAndCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenAndCrcBuf[:], buf...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}
	err = mf.f.Sync()
	return err
}

// 检查manifest文件的正确性，保证db已正确状态进行启动
func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	//对比manifest文件与工作目录中的sst文件
	for id := range mf.mainfest.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file not exist for table %d\n", id)
		}
	}

	//删除非重合部分
	for id := range idMap {
		if _, ok := mf.mainfest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("Table file %d referenced in MANIFEST\n", id))
			filename := utils.GenSSTPath(mf.opt.Dir, id)
			if err := os.Remove(filename); err != nil {
				return errors.Wrapf(err, "remove table %d error\n", id)
			}
		}
	}
	return nil
}

func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.mainfest
}
