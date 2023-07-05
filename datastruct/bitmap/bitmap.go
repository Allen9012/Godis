package bitmap

/*
	@author: Allen
	@since: 2023/3/7
	@desc: //位图
*/

type BitMap []byte

func New() *BitMap {
	b := BitMap(make([]byte, 0))
	return &b
}

// toByteSize
//
//	@Description: 有几个字节
//
// @param bitSize
//
//	@return int64
func toByteSize(bitSize int64) int64 {
	if bitSize%8 == 0 {
		return bitSize / 8
	}
	return bitSize/8 + 1
}

// grow
//
//	@Description: 扩容
//	@receiver b
//	@param bitSize
func (b *BitMap) grow(bitSize int64) {
	// 希望扩容到的字节数
	byteSize := toByteSize(bitSize)
	gap := byteSize - int64(len(*b))
	if gap <= 0 {
		return
	}
	*b = append(*b, make([]byte, gap)...)
}

// BitSize
//
//	@Description: 多大bit
//	@receiver b
//	@return int
func (b *BitMap) BitSize() int {
	return len(*b) * 8
}

// FromBytes
//
//	@Description: byte转化bm
//	@param bytes
//	@return *BitMap
func FromBytes(bytes []byte) *BitMap {
	bm := BitMap(bytes)
	return &bm
}

func (b *BitMap) ToBytes() []byte {
	return *b
}

func (b *BitMap) SetBit(offset int64, val byte) {
	byteIndex := offset / 8
	bitOffset := offset % 8
	// 第几个字节的第几位
	mask := byte(1 << bitOffset)
	// 保证足够大
	b.grow(offset + 1)
	if val > 0 {
		// set bit
		(*b)[byteIndex] |= mask
	} else {
		// clear bit
		(*b)[byteIndex] &^= mask
	}
}

func (b *BitMap) GetBit(offset int64) byte {
	byteIndex := offset / 8
	bitOffset := offset % 8
	if byteIndex >= int64(len(*b)) {
		return 0
	}
	return ((*b)[byteIndex] >> bitOffset) & 0x01
}

type Callback func(offset int64, val byte) bool

// ForEachBit
//
//	@Description: 遍历操作每个bit
//	@receiver b
//	@param begin
//	@param end
//	@param cb
func (b *BitMap) ForEachBit(begin int64, end int64, cb Callback) {
	offset := begin
	byteIndex := offset / 8
	bitOffset := offset % 8
	for byteIndex < int64(len(*b)) {
		char := (*b)[byteIndex]
		for bitOffset < 8 {
			bit := byte(char >> bitOffset & 0x01)
			if !cb(offset, bit) {
				return
			}
			bitOffset++
			offset++
			if offset >= end && end != 0 {
				break
			}
		}
		byteIndex++
		bitOffset = 0
		if end > 0 && offset >= end {
			break
		}
	}
}

// ForEachByte
//
//	@Description: 遍历字节
//	@receiver b
//	@param begin
//	@param end
//	@param cb
func (b *BitMap) ForEachByte(begin int, end int, cb Callback) {
	if end == 0 {
		end = len(*b)
	} else if end > len(*b) {
		end = len(*b)
	}
	for i := begin; i < end; i++ {
		if !cb(int64(i), (*b)[i]) {
			return
		}
	}
}
