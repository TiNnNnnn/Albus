package utils

const (
	MAXHEIGHT = 20
)

type SkipListNode struct {
	//将value offset 与 value size 合并编码，方便实现CAS操作
	//低31位存放offset,高31位存放size
	// value offset: uint32(bits 0-31)
	// value size  : uint32(bits 32-63)
	value uint64

	//尽量减少key的内存占用
	keyOffset uint32 //不可变
	keySize   uint16 //不可变

	//节点实际高度(指针个数)
	height uint16

	//skiplist节点的next指针数组，默认初始化为MAXHEIGHT
	tower [MAXHEIGHT]uint32
}

type SkipList struct {
	height     int32
	headOffset uint32
	ref        int32
	arena      *Arena
	OnClose    func()
}

func newNode(arena *Arena,key []byte,v ValueStruct,height int)*SkipListNode{
	nodeOffset :=arena.addNode(height)
	
}


