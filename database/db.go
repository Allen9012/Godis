/**
  @author: Allen
  @since: 2023/2/25
  @desc: //DB
**/
package database

import (
	"Gedis/datastruct/dict"
	"Gedis/interface/database"
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

// SET K V -> arity = 3
// EXISTS k1 k2 k3 k4 ... arity = -2 表示可以超过
// 校验是否arity合法
func validateArity(arity int, cmdArgs [][]byte) bool {
	argLen := len(cmdArgs)
	if arity >= 0 {
		return argLen == arity
	}
	// arity < 0 说明参数数量可变
	return argLen >= -arity
}

/* ---- data Access ----- */
// 下面的方法相当于对dict套了一层壳

//
// GetEntity returns DataEntity bind to given key
//  @Description: Get
//  @receiver db
//  @param key
//  @return *database.DataEntity
//  @return bool
//
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	//raw是空接口，需要根据实际类型转化
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

//
// PutEntity a DataEntity into DB
//  @Description: Set
//  @receiver db
//  @param key
//  @param entity
//  @return int 存入多少个
//
func (db *DB) PutEntity(key string, entity database.DataEntity) int {
	// 存的时候会自动转化空接口，取的时候需要自己转化
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

// Removes the given keys from db
//
// Removes the given keys from db
//  @Description:
//  @receiver db
//  @param keys 变长参数
//  @return deleted
//
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	db.data.Clear()
}
