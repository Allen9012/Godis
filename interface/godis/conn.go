package godis

// redis的链接

type Connection interface {
	Write([]byte) (int, error)
	GetDBIndex() int // 获取DB编号
	SelectDB(int)    // 选择DB
	Close() error
	RemoteAddr() string
	// TODO password
	//SetPassword(string)
	//GetPassword() string

	//// TODO pubsub
	//// client should keep its subscribing channels
	//Subscribe(channel string)
	//UnSubscribe(channel string)
	//SubsCount() int
	//GetChannels() []string
	//
	//InMultiState() bool
	//SetMultiState(bool)
	//GetQueuedCmdLine() [][][]byte
	//EnqueueCmd([][]byte)
	//ClearQueuedCmds()
	//GetWatching() map[string]uint32
	//AddTxError(err error)
	//GetTxErrors() []error

	// TODO replicaof
	//SetSlave()
	//IsSlave() bool
	//
	//SetMaster()
	//IsMaster() bool
	//
	//Name() string
}
