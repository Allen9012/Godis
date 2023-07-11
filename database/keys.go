package database

/*
	@author: Allen
	@since: 2023/2/26
	@desc: //TODO
*/
import (
	"github.com/Allen9012/Godis/aof"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/utils"
	"github.com/Allen9012/Godis/lib/wildcard"
	"strconv"
	"time"
)

func init() {
	//DEL key [key ...]
	registerCommand("Del", execDel, -2, flagWrite)
	//EXISTS key [key ...]
	registerCommand("Exists", execExists, -2, flagReadOnly)
	//KEYS pattern
	registerCommand("Keys", execKeys, 2, flagReadOnly)
	//FLUSHDB [ASYNC | SYNC]
	registerCommand("FlushDB", execFlushDB, -1, flagReadOnly)
	//TYPE key
	registerCommand("Type", execType, 2, flagReadOnly)
	//RENAME key newkey
	registerCommand("Rename", execRename, 3, flagReadOnly)
	//RENAMENX key newkey
	registerCommand("RenameNx", execRenameNx, 3, flagReadOnly)
	registerCommand("Expire", execExpire, 3, flagWrite)
	registerCommand("ExpireAt", execExpireAt, 3, flagWrite)
	registerCommand("ExpireTime", execExpireTime, 2, flagReadOnly)
	registerCommand("TTL", execTTL, 2, flagReadOnly)
	registerCommand("Persist", execPersist, 2, flagWrite)
	registerCommand("PTTL", execPTTL, 2, flagReadOnly)
	registerCommand("PExpire", execPExpire, 3, flagWrite)
	registerCommand("PExpireAt", execPExpireAt, 3, flagWrite)
	registerCommand("PExpireTime", execPExpireTime, 2, flagReadOnly)
}

func execPExpireTime(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	rawExpireTime, _ := raw.(time.Time)
	expireTime := rawExpireTime.UnixMilli()
	return protocol.MakeIntReply(expireTime)
}

func execPExpireAt(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(0, raw*int64(time.Millisecond))

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)

	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

func execPExpire(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(ttlArg) * time.Millisecond

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

/*--- TTL 相关---*/

// execPTTL
//
//	@Description: PTTL key
//	@param db
//	@param args
//	@return redis.Reply
func execPTTL(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return protocol.MakeIntReply(int64(ttl / time.Millisecond))
}

// execPersist
//
//	@Description:PERSIST key
//	@param db
//	@param args
//	@return redis.Reply
func execPersist(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	_, exists = db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Persist(key)
	db.addAof(utils.ToCmdLine3("persist", args...))
	return protocol.MakeIntReply(1)
}

// execExpireTime
//
//	@Description: EXPIRETIME key
//	@param db
//	@param args
//	@return redis.Reply
func execExpireTime(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	rawExpireTime, _ := raw.(time.Time)
	expireTime := rawExpireTime.Unix()
	return protocol.MakeIntReply(expireTime)
}

// execExpireAt
//
//	@Description: EXPIREAT key unix-time-seconds [NX | XX | GT | LT]
//	@param db
//	@param args
//	@return redis.Reply
func execExpireAt(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execExpire
//
//	@Description: EXPIRE key seconds [NX | XX | GT | LT]
//	@param db
//	@param args
//	@return redis.Reply
func execExpire(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(ttlArg) * time.Second
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttl)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execTTL returns a key's time to live in seconds
//
//	@Description: TTL key
//	@param db
//	@param args
//	@return redis.Reply
func execTTL(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return protocol.MakeIntReply(int64(ttl / time.Second))
}

//DEL
//EXISTS
//KEYS
//FLUSHDB
//TYPE
//RENAME
//RENAMENX

// execDel removes a key from db
func execDel(db *DB, args [][]byte) godis.Reply {
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
	return protocol.MakeIntReply(int64(deleted))
}

// execExists checks if a is existed in db
func execExists(db *DB, args [][]byte) godis.Reply {
	count := int64(0)
	for _, arg := range args {
		// 每拿到一个key就去查看是否有key
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			count++
		}
	}
	return protocol.MakeIntReply(count)
}

// execFlushDB removes all data in current db
func execFlushDB(db *DB, args [][]byte) godis.Reply {
	db.Flush()
	//aof
	db.addAof(utils.ToCmdLine3("flushdb", args...))
	return protocol.MakeOkReply()
}

// execType returns the type of entity, including: string, list, hash, set and zset
func execType(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeStatusReply("none")
	}
	// TODO 增加类型
	switch entity.Data.(type) {
	case []byte:
		return protocol.MakeStatusReply("string")
	}
	// 未知类型默认reply
	return &protocol.UnknownErrReply{}
}

// @Description: execRename a key
// @param db
// @param args
// @return resp.Reply	"OK"
func execRename(db *DB, args [][]byte) godis.Reply {
	//RENAME key newkey
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply("no such key")
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
	return protocol.MakeOkReply()
}

// @Description: execRenameNx a key, only if the new key does not exist
// @param db
// @param args
// @return resp.Reply	1 if key was renamed to newkey. 0 if newkey already exists.
func execRenameNx(db *DB, args [][]byte) godis.Reply {
	//RENAMENX key newkey
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'renamenx' command")
	}
	src := string(args[0])
	dest := string(args[1])
	_, ok := db.GetEntity(dest)
	if ok { // exist不满足
		return protocol.MakeIntReply(0)
	}
	entity, ok := db.GetEntity(src)
	if !ok { // 找不到原来的key
		return protocol.MakeErrReply("no such key")
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
	return protocol.MakeIntReply(1)
}

//	@Description: execKeys returns all keys matching the given pattern
//	@param db
//	@param args
//	@return redis.Reply
//
// 需要借助第三方库实现通配符
func execKeys(db *DB, args [][]byte) godis.Reply {
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
	return protocol.MakeMultiBulkReply(result)
}
