package lsm

import (
	"albus/file"
	"albus/utils"
	"os"
)

type table struct {
	sst *file.SSTable
	lm  *levelManager
	fid uint64
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sst := file.OpenSST(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkerDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSize:  int(lm.opt.SSTableMaxSize),
	})
	t := &table{
		sst: sst,
		lm:  lm,
		fid: utils.GetFidByPath(tableName),
	}
	if builder != nil {
		if err := builder.flush(sst); err != nil {
			utils.Err(err)
			return nil
		}
	}
	if err := t.sst.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	return t
}
