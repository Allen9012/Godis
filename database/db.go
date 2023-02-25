/**
  @author: Allen
  @since: 2023/2/25
  @desc: //DB
**/
package database

import (
	"Gedis/datastruct/dict"
	"Gedis/interface/resp"
	"Gedis/resp/reply"
	"strings"
)

// DB stores data and execute user's commands
type DB struct {
	index int
	data  dict.Dict
}

// ExecFunc 统一执行方法
// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) resp.Reply

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
	}
	return db
}

//
// Exec executes command within one database
//  @Description:
//  @receiver db*
//  @param connection
//  @param cmdline
//
func (db *DB) Exec(connection resp.Connection, cmdLine CmdLine) resp.Reply {
	// 用户发的是什么指令
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command " + cmdName)
	}
	// 校验arity是否合法
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.exector
	// SET K V ->K V
	return fun(db, cmdLine)
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	return true
}
