/**
  @author: Allen
  @since: 2023/2/26
  @desc: //TODO
**/
package database

import (
	"Gedis/interface/resp"
	"Gedis/lib/utils"
	"Gedis/lib/wildcard"
	"Gedis/resp/reply"
)

func init() {
	//DEL key [key ...]
	RegisterCommand("Del", execDel, -2)
	//EXISTS key [key ...]
	RegisterCommand("Exists", execExists, -2)
	//KEYS pattern
	RegisterCommand("Keys", execKeys, 2)
	//FLUSHDB [ASYNC | SYNC]
	RegisterCommand("FlushDB", execFlushDB, -1)
	//TYPE key
	RegisterCommand("Type", execType, 2)
	//RENAME key newkey
	RegisterCommand("Rename", execRename, 3)
	//RENAMENX key newkey
	RegisterCommand("RenameNx", execRenameNx, 3)
}

//DEL
//EXISTS
//KEYS
//FLUSHDB
//TYPE
//RENAME
//RENAMENX

// execDel removes a key from db
func execDel(db *DB, args [][]byte) resp.Reply {
	// 把[][]args->keys
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	//aof
	if deleted > 0 {
		db.addAof(utils.ToCmdLine2("del", args...))
	}
	return reply.MakeIntReply(int64(deleted))
}

// execExists checks if a is existed in db
func execExists(db *DB, args [][]byte) resp.Reply {
	count := int64(0)
	for _, arg := range args {
		// 每拿到一个key就去查看是否有key
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			count++
		}
	}
	return reply.MakeIntReply(count)
}

// execFlushDB removes all data in current db
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	//aof
	db.addAof(utils.ToCmdLine2("flushdb", args...))
	return reply.MakeOkReply()
}

// execType returns the type of entity, including: string, list, hash, set and zset
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}
	// TODO 增加类型
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	}
	// 未知类型默认reply
	return &reply.UnknowErrReply{}
}

//
//  @Description: execRename a key
//  @param db
//  @param args
//  @return resp.Reply	"OK"
//
func execRename(db *DB, args [][]byte) resp.Reply {
	//RENAME key newkey
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}

	src := string(args[0])
	dest := string(args[1])
	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	//aof
	db.addAof(utils.ToCmdLine2("rename", args...))
	return reply.MakeOkReply()
}

//
//  @Description: execRenameNx a key, only if the new key does not exist
//  @param db
//  @param args
//  @return resp.Reply	1 if key was renamed to newkey. 0 if newkey already exists.
//
func execRenameNx(db *DB, args [][]byte) resp.Reply {
	//RENAMENX key newkey
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'renamenx' command")
	}
	src := string(args[0])
	dest := string(args[1])
	_, ok := db.GetEntity(dest)
	if ok { // exist不满足
		return reply.MakeIntReply(0)
	}
	entity, ok := db.GetEntity(src)
	if !ok { // 找不到原来的key
		return reply.MakeErrReply("no such key")
	}
	db.Remove(src)
	db.PutEntity(dest, entity)
	//aof
	db.addAof(utils.ToCmdLine2("renamenx", args...))
	return reply.MakeIntReply(1)
}

//
//  @Description: execKeys returns all keys matching the given pattern
//  @param db
//  @param args
//  @return resp.Reply
// 需要借助第三方库实现通配符
func execKeys(db *DB, args [][]byte) resp.Reply {
	// 拿到通配符
	pattern := wildcard.CompilePattern(string(args[0]))
	// 返回用初始化二维切片
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}
