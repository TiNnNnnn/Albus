package lsm

import (
	"albus/utils"
	"errors"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// 压缩优先级
// L0: 根据sst文件数量决定
// LN: 根据每个level的除去正在压缩的sst文件与每层的期望size的比值作为优先级

// 注：每个level的期望size之间差一个数量级
type comapactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

// 归并目标
type targets struct {
	baseLevel int
	targetSz  []int64
	fileSz    []int64
}

type compactDef struct {
	compactorId int
	t           targets
	p           comapactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	thisRange keyRange
	nextRange keyRange
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64
}

func (lm *levelManager) runCompact(id int) {
	defer lm.lsm.closer.Done()
	//创建随机时延
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000) * int32(time.Millisecond)))
	select {
	case <-randomDelay.C:

	case <-lm.lsm.closer.Wait():
		randomDelay.Stop()
		return
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.Wait():
			return
		}
	}
}

func (lm *levelManager) runOnce(id int) bool {
	priors := lm.pickCompactLevels()
	if id == 0 {
		//0号协程，倾向于压缩L0层
		priors = moveL0toFront(priors)
	}
	for _, p := range priors {
		if id == 0 && p.level == 0 {
			//L0层必须处理
		} else if p.adjusted < 1.0 {
			//其他level得分小于1.0，不做不处理
			break
		}
		if lm.run(id, p) {
			return true
		}
	}
	return false
}

func moveL0toFront(priors []comapactionPriority) []comapactionPriority {
	idx := -1
	for i, p := range priors {
		if p.level == 0 {
			idx = i
			break
		}
	}
	//idx = -1,找不到L0
	//idx = 0,L0已经在队首，不移动
	//idx > 0,移动L0优先级到最开头
	if idx > 0 {
		out := append([]comapactionPriority{}, priors[idx])
		out = append(out, priors[:idx]...)   //0-->idx-1
		out = append(out, priors[idx+1:]...) //idx+1-->end
		return out
	}
	return priors
}

// 选择level并执行，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (priors []comapactionPriority) {
	t := lm.levelTargets()
	addPriority := func(level int, score float64) {
		pri := comapactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		priors = append(priors, pri)
	}
	//根据L0的table数量进行压缩提权
	//score越大，压缩优先级越高
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))

	//非Lo层计算优先级，根据level总大小
	for i := 1; i < len(lm.levels); i++ {
		//获取正在进行压缩的sst大小，这部分不能纳入压缩优先级计算
		delsize := lm.compactStatus.delSize(i)
		l := lm.levels[i]
		sz := l.getTotalSize() - delsize
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	utils.CondPanic(len(priors) != len(lm.levels), errors.New("pickCompactLevels error: len of priors error"))
	//对score进行调整
	var prevLevel int
	for level := t.baseLevel; level < len(lm.levels); level++ {
		if priors[prevLevel].adjusted >= 1 {
			const minScore = 0.01
			if priors[level].score >= minScore {
				priors[prevLevel].adjusted /= priors[level].adjusted
			} else {
				priors[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	//仅选择得分大于1的压缩内容，允许L0到L0的特殊压缩
	out := priors[:0]
	for _, p := range priors[:len(priors)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	priors = out

	//按照优先级排序
	sort.Slice(priors, func(i, j int) bool {
		return priors[i].adjusted > priors[j].adjusted
	})
	return priors
}

// 执行一个指定优先级的合并任务
func (lm *levelManager) run(id int, p comapactionPriority) bool {
	err := lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
	default:
		log.Printf("[taskId:%d] run doComapact failed: %v\n", id, err)
	}
	return false
}

func (lm *levelManager) doCompact(id int, p comapactionPriority) error {
	
	return nil
}

func (lm *levelManager) levelTargets() targets {
	//调整函数：调整level的期望值
	adjust := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}

	//初始化默认都是最大层数
	//targetSz: Lx层预期sst总字节数
	//fileSz: Lx层预期sst文件大小
	t := targets{
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}
	dbSize := lm.lastLevelHandler().getTotalSize()
	for i := len(lm.levels) - 1; i > 0; i-- {
		levelTargetSize := adjust(dbSize)
		t.targetSz[i] = levelTargetSize

		if t.baseLevel == 0 && levelTargetSize <= lm.opt.BaseLevelSize {
			//设置当前level为压缩目标层
			t.baseLevel = i
		}
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}
	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	b := t.baseLevel
	lvl := lm.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 &&
		lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

func (lm *levelManager) lastLevelHandler() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}
