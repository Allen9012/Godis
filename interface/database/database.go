/*
*

	@author: Allen
	@since: 2023/2/24
	@desc: // 代表redis的业务核心

*
*/
package database

import (
	"github.com/Allen9012/Godis/interface/godis"
	"time"
)

type CmdLine = [][]byte

type Database interface {
	Exec(client godis.Connection, args [][]byte) godis.Reply //执行操作，回复响应
	Close()                                                  //关闭
	AfterClientClose(c godis.Connection)                     //删除后数据清理
}

// DBEngine is the embedding storage engine exposing more methods for complex application
type DBEngine interface {
	Database
	ExecWithLock(conn godis.Connection, cmdLine [][]byte) godis.Reply
	ExecMulti(conn godis.Connection, watching map[string]uint32, cmdLines []CmdLine) godis.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
}

type DataEntity struct {
	Data interface{}
}
