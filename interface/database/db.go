package database

/*
	@author: Allen
	@since: 2023/2/24
	@desc: // 代表godis的业务核心
*/
import (
	"github.com/Allen9012/Godis/interface/godis"
	"time"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

type DB interface {
	Exec(client godis.Connection, args [][]byte) godis.Reply //执行操作，回复响应
	Close()                                                  //关闭
	AfterClientClose(c godis.Connection)                     //删除后数据清理
	//LoadRDB(dec *core.Decoder) error
}

// DBEngine is the embedding storage engine exposing more methods for complex application
type DBEngine interface {
	DB
	ExecWithLock(conn godis.Connection, cmdLine [][]byte) godis.Reply
	// ExecMulti(conn godis.Connection, watching map[string]uint32, cmdLines []CmdLine) godis.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
	GetEntity(dbIndex int, key string) (*DataEntity, bool)
	GetExpiration(dbIndex int, key string) *time.Time
	//SetKeyInsertedCallback(cb KeyEventCallback)
	//SetKeyDeletedCallback(cb KeyEventCallback)
}

type DataEntity struct {
	Data interface{}
}
