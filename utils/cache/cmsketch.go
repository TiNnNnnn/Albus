package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	cmDepth = 4
)

// 一个计数器
type cmRow []byte

type CmSketch struct {
	//cmDepth个计数器
	rows [cmDepth]cmRow
	//cmDepth个随机种子
	seed [cmDepth]uint64
	mask uint64
}

func newCmSketch(numCounters int64) *CmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid num counters")
	}
	numCounters = next2Power(numCounters)

	sketch := &CmSketch{
		mask: uint64(numCounters - 1),
	}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmdRow(numCounters)
	}
	return nil
}

func (s *CmSketch) Increment(hashed uint64) {
	for i := range s.rows {
		idx := (hashed ^ s.seed[i]) & s.mask
		s.rows[i].increment(idx)
	}
}

func (s *CmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := range s.rows {
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if val < min {
			min = val
		}
	}
	return int64(min)
}

func (s *CmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

func (r cmRow) get(n uint64) byte {
	return r[n/2] >> ((n & 1) * 4) & 0x0f
}

func (r cmRow) reset() {
	for i := range r {
		r[i] = (r[i] >> 1) & 0x77
	}
}

func (r cmRow) increment(n uint64) {
	i := n / 2
	s := (n & 1) * 4
	v := (r[i] >> s) & 0x0f
	if v < 15 {
		r[i] += 1 << s
	}
}

func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}

func newCmdRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

func (r cmRow) string() string {
	s := ""
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		s += fmt.Sprintf("%02d", (r[i/2])>>((i&1)*4)&0x0f)
	}
	return s
}

func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}
