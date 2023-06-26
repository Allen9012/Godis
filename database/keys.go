/*
*

	@author: Allen
	@since: 2023/2/26
	@desc: //TODO

*
*/
package database

import (
	"github.com/Allen9012/Godis/aof"
	"github.com/Allen9012/Godis/interface/resp"
	"github.com/Allen9012/Godis/lib/utils"
	"github.com/Allen9012/Godis/lib/wildcard"
	"github.com/Allen9012/Godis/resp/reply"
	"strconv"
	"time"
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

	RegisterCommand("RenameNx", execRenameNx, 3)
	RegisterCommand("Expire", execExpire, 3)
	RegisterCommand("ExpireAt", execExpireAt, 3)
	RegisterCommand("ExpireTime", execExpireTime, 2)
	RegisterCommand("TTL", execTTL, 2)
	RegisterCommand("Persist", execPersist, 2)
	RegisterCommand("PTTL", execPTTL, 2)
	RegisterCommand("PExpire", execPExpire, 3)
	RegisterCommand("PExpireAt", execPExpireAt, 3)
	RegisterCommand("PExpireTime", execPExpireTime, 2)
}

func execPExpireTime(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return reply.MakeIntReply(-1)
	}
	rawExpireTime, _ := raw.(time.Time)
	expireTime := rawExpireTime.UnixMilli()
	return reply.MakeIntReply(expireTime)
}

func execPExpireAt(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(0, raw*int64(time.Millisecond))

	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	db.Expire(key, expireAt)

	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return reply.MakeIntReply(1)
}

func execPExpire(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(ttlArg) * time.Millisecond

	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return reply.MakeIntReply(1)
}

/*--- TTL 相关---*/

// execPTTL
//
//	@Description: PTTL key
//	@param db
//	@param args
//	@return resp.Reply
func execPTTL(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return reply.MakeIntReply(-1)
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return reply.MakeIntReply(int64(ttl / time.Millisecond))
}

// execPersist
//
//	@Description:PERSIST key
//	@param db
//	@param args
//	@return resp.Reply
func execPersist(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	_, exists = db.ttlMap.Get(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	db.Persist(key)
	db.addAof(utils.ToCmdLine3("persist", args...))
	return reply.MakeIntReply(1)
}

// execExpireTime
//
//	@Description: EXPIRETIME key
//	@param db
//	@param args
//	@return resp.Reply
func execExpireTime(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return reply.MakeIntReply(-1)
	}
	rawExpireTime, _ := raw.(time.Time)
	expireTime := rawExpireTime.Unix()
	return reply.MakeIntReply(expireTime)
}

// execExpireAt
//
//	@Description: EXPIREAT key unix-time-seconds [NX | XX | GT | LT]
//	@param db
//	@param args
//	@return resp.Reply
func execExpireAt(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return reply.MakeIntReply(1)
}

// execExpire
//
//	@Description: EXPIRE key seconds [NX | XX | GT | LT]
//	@param db
//	@param args
//	@return resp.Reply
func execExpire(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(ttlArg) * time.Second
	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return reply.MakeIntReply(1)
}

// execTTL returns a key's time to live in seconds
//
//	@Description: TTL key
//	@param db
//	@param args
//	@return resp.Reply
func execTTL(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return reply.MakeIntReply(-1)
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return reply.MakeIntReply(int64(ttl / time.Second))
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
		db.addAof(utils.ToCmdLine3("del", args...))
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
	db.addAof(utils.ToCmdLine3("flushdb", args...))
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

// @Description: execRename a key
// @param db
// @param args
// @return resp.Reply	"OK"
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
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.PutEntity(dest, entity)
	db.Remove(src)
	if hasTTL {
		db.Persist(src) // clean src and dest with their ttl
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLine3("rename", args...))
	return reply.MakeOkReply()
}

// @Description: execRenameNx a key, only if the new key does not exist
// @param db
// @param args
// @return resp.Reply	1 if key was renamed to newkey. 0 if newkey already exists.
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
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.Removes(src)
	db.PutEntity(dest, entity)
	if hasTTL {
		db.Persist(src) // clean src and dest with their ttl
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	//aof
	db.addAof(utils.ToCmdLine3("renamenx", args...))
	return reply.MakeIntReply(1)
}

//	@Description: execKeys returns all keys matching the given pattern
//	@param db
//	@param args
//	@return resp.Reply
//
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
