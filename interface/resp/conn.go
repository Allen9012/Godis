package resp

// redis的链接

type Connection interface {
	Write([]byte) error
	GetDBIndex() int // 获取DB编号
	SelectDB(int)    // 选择DB
}
