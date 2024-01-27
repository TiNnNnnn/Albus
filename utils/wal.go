package utils

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
)

// ValuePtr:for vlog
type LogEntry func(e *Entry, vp *ValuePtr) error

const maxHeaderSize int = 21

type WalHeader struct {
	KeyLen      uint32
	ValueLen    uint32
	ExpirationT uint64
}

func (h *WalHeader) EncodeWalHeader(in []byte) int {
	idx := 0
	idx = binary.PutUvarint(in[idx:], uint64(h.KeyLen))
	idx += binary.PutUvarint(in[idx:], uint64(h.ValueLen))
	idx += binary.PutUvarint(in[idx:], h.ExpirationT)
	return idx
}

func (h *WalHeader) DecodeWalHeader(reader *HashReader) (int, error) {
	var err error
	keylen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(keylen)
	valuelen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(valuelen)
	h.ExpirationT, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

// WalFile Format: |header|key|value|crc32|
// 将entry编码为字节数组
func WalEncode(buf *bytes.Buffer, e *Entry) int {
	buf.Reset()
	h := WalHeader{
		KeyLen:      uint32(len(e.Key)),
		ValueLen:    uint32(len(e.Value)),
		ExpirationT: e.ExpirationT,
	}
	hash := crc32.New(CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	//序列化header
	var headerEnc [maxHeaderSize]byte
	hsz := h.EncodeWalHeader(headerEnc[:])
	_, err := writer.Write(headerEnc[:hsz])
	Panic(err)
	_, err = writer.Write(e.Key)
	Panic(err)
	_, err = writer.Write(e.Value)
	Panic(err)

	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	_, err = buf.Write((crcBuf[:]))
	Panic(err)

	return len(headerEnc[:hsz]) + len(e.Key) + len(e.Value) + len(crcBuf)

}

/*
接口类：
type Reader interface {
    Read(p []byte) (n int, err error)
}
*/
// 读取数据，同时计算数据哈希值
type HashReader struct {
	Reader    io.Reader
	Hash      hash.Hash32
	BytesRead int
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)
	return &HashReader{
		Reader: r,
		Hash:   hash,
	}
}

func (hr *HashReader) Read(b []byte) (int, error) {
	n, err := hr.Reader.Read(b)
	if err != nil {
		return n, err
	}
	hr.BytesRead += n
	return hr.Hash.Write(b[:n])
}

func (hr *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := hr.Read(b)
	return b[0], err
}

func (hr *HashReader) Sum32() uint32 {
	return hr.Hash.Sum32()
}

// entry.Key len ? 0
func (e *Entry) IsKeyZero() bool {
	return len(e.Key) == 0
}

func (e *Entry) LogHeaderLen() int {
	return e.Hlen
}

func (e *Entry) LogOffset() uint32 {
	return e.Offset
}
