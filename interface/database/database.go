/**
  @author: Allen
  @since: 2023/2/24
  @desc: // 代表redis的业务核心
**/
package database

import (
	"Gedis/interface/resp"
	"time"
)

type CmdLine = [][]byte

type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply //执行操作，回复响应
	Close()                                                //关闭
	AfterClientClose(c resp.Connection)                    //删除后数据清理
}

// DBEngine is the embedding storage engine exposing more methods for complex application
type DBEngine interface {
	Database
	ExecWithLock(conn resp.Connection, cmdLine [][]byte) resp.Reply
	ExecMulti(conn resp.Connection, watching map[string]uint32, cmdLines []CmdLine) resp.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
}

type DataEntity struct {
	Data interface{}
}
