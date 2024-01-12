package utils

import "sync"

// 管理goruntinue 的关闭与等待
type Closer struct {
	wg sync.WaitGroup
	//发送关闭信号
	closeSignal chan struct{}
}

// create closer
// args: goruntinue num
func NewCloser(i int) *Closer {
	closer := &Closer{
		wg:          sync.WaitGroup{},
		closeSignal: make(chan struct{}),
	}
	closer.wg.Add(i)
	return closer
}

// close all goroutinue
func (c *Closer) Close() {
	close(c.closeSignal)
	c.wg.Wait()
}

// sub waitgroup counter ，表示一个goroutinue finsih
func (c *Closer) Done() {

	c.wg.Done()
}

func (c *Closer) Wait() chan struct{} {
	//goroutinue wait until signal come
	return c.closeSignal
}
