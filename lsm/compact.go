package lsm

import (
	"albus/utils"
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
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

// 执行计划定义
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
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
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

// 触发一个compact任务
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
			//L0层不考虑得分 必须处理
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
	l := p.level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, utils.ErrFillTables)
	if p.t.baseLevel == 0 {
		p.t = lm.levelTargets()
	}

	//创建压缩计划
	cd := compactDef{
		compactorId:  id,
		p:            p,
		t:            p.t,
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
	}

	if l == 0 {
		cd.nextLevel = lm.levels[p.t.baseLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		if !cd.thisLevel.isLastLevel() {
			//指定下一层为到达层
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}
	//完成合并工作，从合并状态中删除
	defer lm.compactStatus.delete(cd)

	//执行合并计划
	if err := lm.runCompactDef(id, l, cd); err != nil {
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil

}

func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unLockLevels()

	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	//当前层已经是最后一层，直接自压缩
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	lm.sortByHeuristic(tables, cd)
	//当前层不是最后一层，直接向thisLevel+1层压缩
	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange()
		//有冲突，该sst已经在压缩
		if lm.compactStatus.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}
		//确定top sst
		cd.top = []*table{t}
		//确定bot sst
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*table{}
			cd.nextRange = cd.thisRange
			if !lm.compactStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		} else {
			cd.nextRange = getKeyRange(cd.bot...)
			if lm.compactStatus.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
				continue
			}
			if !lm.compactStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
		}
	}
	return false
}

// 启发式排序(From RocksDB): 优先压缩旧sst
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	//按照sst最大版本升序排序 （旧版本放前面）
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].sst.GetIndexs().MaxVersion < tables[j].sst.GetIndexs().MaxVersion
	})
}

// 先尝试L0到Lbase的压缩，失败则L0到L0压缩
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTbalesL02Lbase(cd); ok {
		return true
	} else {
		return lm.fillTablesL0T0L0(cd)
	}
}

func (lm *levelManager) fillTbalesL02Lbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("fillTbalesL02Lbase error: nextLevel=0"))
	}
	//优先级低于1，不执行
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		return false
	}
	cd.lockLevels()
	defer cd.unLockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange

	for _, t := range top {
		planKr := getKeyRange(t)
		if kr.overlapsWith(planKr) {
			out = append(out, t)
			kr.extend(planKr)
		} else {
			break
		}
	}
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		//bot为空，则尝试向0层去压缩
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return lm.compactStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (lm *levelManager) fillTablesL0T0L0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		//0号线程专门负责执行L02L0，避免L0到L0的资源竞争
		return false
	}
	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	utils.CondPanic(cd.thisLevel.levelNum != 0, utils.ErrLevelNum)
	utils.CondPanic(cd.nextLevel.levelNum != 0, utils.ErrLevelNum)

	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.compactStatus.Lock()
	defer lm.compactStatus.Unlock()

	top := cd.thisLevel.tables
	var out []*table

	now := time.Now()
	for _, t := range top {
		//sst文件大于预期fileSz两倍，说明压缩过
		//L02L0的压缩，不对过大的sst压缩，避免性能抖动
		if t.Size() >= 2*cd.t.fileSz[0] {
			continue
		}
		//sst创建时间小于10s，不做处理
		if now.Sub(*t.sst.GetCreateAt()) < 10*time.Second {
			continue
		}
		//当前sst已经在压缩状态，跳过
		if _, isCompacting := lm.compactStatus.tables[t.fid]; isCompacting {
			continue
		}
		out = append(out, t)
	}
	//sst数量小于4,不进行压缩
	if len(out) < 4 {
		return false
	}

	cd.thisRange = infRange
	cd.top = out

	//该过程应该避免任何L02Lx的压缩合并
	thisLevel := lm.compactStatus.levels[cd.thisLevel.levelNum]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactStatus.tables[t.fid] = struct{}{}
	}
	//L02L0最终压缩为一个文件
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

// maxLevel的自压缩
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		//无过期信息
		return false
	}
	cd.bot = []*table{}
	//收集sst函数
	collectBotTables := func(t *table, needSz int64) {
		tableSize := t.Size()
		//二分查找目标table
		j := sort.Search(len(tables), func(i int) bool {
			return utils.CompareKeys(tables[i].sst.GetMinKey(), t.sst.GetMinKey()) >= 0
		})
		utils.CondPanic(tables[j].fid != t.fid, errors.New("fillMaxLevelTables err: tables[j].fid != t.fid"))
		j++
		//收集目标table之后的table，直到满足needSz
		for j < len(tables) {
			newT := tables[j]
			tableSize += newT.Size()
			if tableSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}

	now := time.Now()
	for _, t := range sortedTables {
		//sst创建时间少于1h,不选择
		if now.Sub(*t.CreateAt()) < time.Hour {
			continue
		}
		//sst大小小于1MB，不选择
		if t.StaleDataSize() < 10<<20 {
			continue
		}
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		//该sst正在压缩，跳过
		if lm.compactStatus.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		cd.top = []*table{t}

		needFileSize := cd.t.fileSz[cd.thisLevel.levelNum]
		//sst大于我们的taget fileSz,满足压缩条件
		if t.Size() >= needFileSize {
			break
		}
		//tablsSize 少于我们的target fileSz,收集更多的sst进行压缩
		collectBotTables(t, needFileSize)
		if !lm.compactStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}
	return lm.compactStatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})

}

func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))
	if thisLevel == nextLevel {
		//l02l0,lmax2lmax 不进行处理
	} else {
		lm.addSplits(&cd)
	}
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	newTables,decr,err := 

	return nil
}

func(lm *levelManager)compactBuildTables(level int,cd compactDef)([]*table,func() error,error){
	topTables := cd.top
	botTables := cd.bot 
	iterOpt := &utils.Options{
		IsAsc: true,
	}

	newIterator := func() []utils.Iterator{
		var iter []utils.Iterator
		switch{
		case level == 0:
			iters = append(iters,iteratorReversed(topTables,iterOpt)...)
		case len(topTables) > 0:
			iters = []utils.Iterator{topTables[0].NewTableIterator(iterOpt)}
		}
		return append(iters,)
	}
}

func iteratorReversed(ts []*table,opt *utils.Options)[]utils.Iterator{
	out := make([]utils.Iterator,0,len(ts))
	for i:= len(ts)-1;i>=0;i--{
		out = append(out, ts[i].NewTableIterator(opt))
	}
	return out 
}


func (lm *levelManager) addSplits(cd *compactDef){
	cd.splits = cd.splits[:0]

	width := int(math.Ceil(float64(len(cd.bot))/5.0))
	if(width < 3){
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte){
		skr.right = utils.Copy(right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}

	for i,t := range cd.bot{
		if i == len(cd.bot)-1{
			addRange([]byte{})
			return
		}
		if i%width == width -1{
			//设置最大值为右区间
			right := utils.KeyWithTime(utils.ParseKey(t.sst.GetMaxKey()),math.MaxUint64)
			addRange(right)
		}
	}
}

// 获取tableList的keyRange,即[minKey,maxKey]
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	minKey := tables[0].sst.GetMinKey()
	maxKey := tables[0].sst.GetMaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].sst.GetMinKey(), minKey) < 0 {
			minKey = tables[i].sst.GetMinKey()
		}
		if utils.CompareKeys(tables[i].sst.GetMaxKey(), maxKey) > 0 {
			maxKey = tables[i].sst.GetMaxKey()
		}
	}
	//版本号降序排列
	return keyRange{
		left:  utils.KeyWithTime(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTime(utils.ParseKey(maxKey), 0),
	}
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
	t := targets{
		targetSz: make([]int64, len(lm.levels)), //targetSz: Lx层预期sst总字节数
		fileSz:   make([]int64, len(lm.levels)), //fileSz: Lx层预期sst文件大小
	}
	//设置每层的期望值 （每层呈现递增趋势）
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
			//L0层设置 memtable size为 fileSz
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			//小于等于 baseLevel，设置fileSz为 basetableSize
			t.fileSz[i] = tsz
		} else {
			//大于baseLevel则依层不断增加，每次扩大tableSizeMutiplier倍
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}
	//找到最后一个空level作为目标level实现归并，减少写放大
	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	b := t.baseLevel
	lvl := lm.levels
	//寻找断层（totalSize == 0），有则设其为baseLevel
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

// 判断当前level的压缩计划(keyrange)是否与进行中压缩冲突 （for compactStatus）
func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

type thisAndNextLevelRLocked struct{}

func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum
	utils.CondPanic(tl >= len(cs.levels), utils.ErrLevelNum)
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]
	//如果当前压缩计划与同一层压缩计划冲突，则暂停
	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}
	//设置range为压缩状态
	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize

	//将压缩状态的sst加入cs中保存
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum

	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)

	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevel.remove(cd.nextRange) && found
	}

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this, tl)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next, cd.nextLevel.levelNum)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("KeyRange not found")
	}

	//将涉及的所有tables都解除压缩状态
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs tables is nil"))
		delete(cs.tables, t.fid)
	}
}

func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals((dst)) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

// 判断当前level的压缩计划是否与进行中压缩冲突
func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, kr := range lcs.ranges {
		if kr.overlapsWith(dst) {
			return true
		}
	}
	return false
}

func (kr *keyRange) isEmpty() bool {
	return len(kr.left) == 0 && len(kr.right) == 0 && !kr.inf
}

var infRange = keyRange{
	inf: true,
}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x,right=%x,inf=%v]", r.left, r.right, r.inf)
}

func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) && bytes.Equal(r.right, dst.right) && r.inf == dst.inf
}

// 将区间r 和 kr 进行合并
// example
// r:	  [------------]
// kr:          [-------------]
// extend；[------------------]
func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || utils.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.right
	}
	if len(r.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}

func (r *keyRange) overlapsWith(dst keyRange) bool {
	if r.isEmpty() {
		return true
	}
	if dst.isEmpty() {
		return true
	}
	//当keyRnage.inf设置为true,表示对整层加范围锁
	if r.inf || dst.inf {
		return true
	}
	if utils.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	if utils.CompareKeys(r.right, dst.left) < 0 {
		return false
	}
	return true
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unLockLevels() {
	cd.thisLevel.Unlock()
	cd.nextLevel.Unlock()
}
