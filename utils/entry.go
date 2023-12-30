package utils

type ValueStruct struct {
	Value []byte
	//过期时间
	ExpirationT uint64
}

func (vs *ValueStruct) EncodeSize() uint32 {
	sz := len(vs.Value) + 1
	ez := sizeOfUint64(vs.ExpirationT)
	return uint32(sz + ez)
}

//对value进行编码，并写入byte数组
//将value与expirationT一起编码
func (vs *ValueStruct) EncodeValue(buf []byte) {
	
}

//将编码的value和expiration进行解码
func (vs *ValueStruct) DncodeValue(buf []byte) {

}

//计算int64类型数据占用内存大小
func sizeOfUint64(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
