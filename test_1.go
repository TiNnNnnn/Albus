package main

import (
	"fmt"
	"math"
	"unsafe"
)

type SkipListNode struct {
	//将value offset 与 value size 合并编码，方便实现CAS操作
	//低31位存放offset,高31位存放size
	// value offset: uint32(bits 0-31)
	// value size  : uint32(bits 32-63)
	value uint64 //8

	//尽量减少key的内存占用
	keyOffset uint32 //不可变 //4
	keySize   uint16 //不可变 //2

	//节点实际高度(指针个数)
	height uint16 //2

	//skiplist节点的next指针数组，默认初始化为MAXHEIGHT
	tower [20]uint32 //4*20
}

func sizeOfInt64(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func test() {
	OFFSETSIZE := int(unsafe.Sizeof(uint32(0)))
	MAXNODESIZE := int(unsafe.Sizeof(SkipListNode{}))
	//内存对齐
	NODEALIGN := int(unsafe.Sizeof(uint64(0))) - 1

	fmt.Printf("OFFSETSIZE: %d\n", OFFSETSIZE)
	fmt.Printf("MAXNODESIZE: %d\n", MAXNODESIZE)
	fmt.Printf("NODEALIGN:  %d\n", NODEALIGN)
}

// 验证切片的深浅拷贝问题
func test_slice() {
	var sl []int = make([]int, 5)
	fmt.Println("sl=", sl)
	s2 := sl[1:3]
	s2[1] = 3
	fmt.Println("s2=", s2)
	fmt.Println("new sl=", sl)
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

func test_random() {
	heightIncrease := math.MaxUint32 / 3
	fmt.Println("heightIncrease=", heightIncrease)
	fmt.Println("rand=", FastRand())
}

//find . -type f -name "*.go" -not -path "./vendor/*" -exec grep -v '^ *//' {} \; -exec grep -v '^ *$' {} \; | wc -l

func main() {
	//test()
	//fmt.Printf("%d", sizeOfInt64(903432))
	//test_slice()
	test_random()
}
