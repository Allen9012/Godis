/**
  @author: Allen
  @since: 2023/2/26
  @desc: //string
**/
package database

import (
	"Gedis/aof"
	"Gedis/interface/database"
	"Gedis/interface/resp"
	"Gedis/lib/logger"
	"Gedis/lib/utils"
	"Gedis/resp/reply"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
	"time"
)

//GET
//SET
//SETNX
//GETSET
//STRLEN
//GETEX
//SETEX
func init() {
	// GET key
	RegisterCommand("Get", execGet, 2)
	// SET key value (只实现最简单的模式)
	RegisterCommand("Set", execSet, -3)
	// SETNX key value
	RegisterCommand("SetNx", execSetNX, 3)
	// GETSET key value
	RegisterCommand("GetSet", execGetSet, 3)
	// STRLEN key
	RegisterCommand("StrLen", execStrLen, 2)
	// GETEX key +
	RegisterCommand("GetEx", execGetEX, -2)
	// SETEX key seconds value
	RegisterCommand("SetEx", execSetEX, 4)
	// INCR key
	RegisterCommand("Incr", execIncr, 2)
	// INCRBY key increment
	RegisterCommand("IncrBy", execIncrBy, 3)
	RegisterCommand("IncrByFloat", execIncrByFloat, 3)
	RegisterCommand("Decr", execDecr, 2)
	RegisterCommand("DecrBy", execDecrBy, 3)

}

//
// getAsString
//  @Description: 简化操作提取公共方法
//  @receiver db
//  @param key
//  @return []byte
//  @return reply.ErrorReply
//
func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

// execGet returns string value bound to the given key
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		logger.Info("execGet can't find value for the key: " + key)
		return reply.MakeNullBulkReply()
	}
	// 第二个是判断是否转换成功
	bytes, ok := entity.Data.([]byte)
	if !ok {
		//TODO 类型转化错误
		return reply.MakeErrReply(" type transfer error")
	}
	return reply.MakeBulkReply(bytes)
}

// 设置TTL
const unlimitedTTL int64 = 0

//
//	execGetEX Get the value of key and optionally set its expiration
//  @Description: 注意需要考虑ttl
//  @param db
//  @param args		GETEX mykey
//  @return resp.Reply
//
//EX seconds: 设置指定的过期时间（以秒为单位）。
//PX milliseconds: 设置指定的过期时间（以毫秒为单位）。
//PERSIST: 删除与键关联的任何现有过期时间。
func execGetEX(db *DB, args [][]byte) resp.Reply {
	// 1. 拿到key的bytes
	// 2. 判断后续参数要求
	key := string(args[0])
	bytes, err := db.getAsString(key)
	ttl := unlimitedTTL
	if err != nil {
		return err
	}
	if bytes == nil {
		return reply.MakeNullBulkReply()
	}
	for i := 1; i < len(args); i++ {
		arg := strings.ToLower(string(args[i]))
		if arg == "ex" { // 秒级单位
			if ttl != unlimitedTTL {
				//ttl has been set
				return reply.MakeSyntaxErrReply()
			}
			// 参数超标不对了
			if i+1 >= len(args) {
				return reply.MakeSyntaxErrReply()
			}
			// 拿到ttl的时间
			ttlArg, err := strconv.ParseInt(string(arg[i+1]), 10, 64)
			if err != nil {
				return reply.MakeSyntaxErrReply()
			}
			if ttlArg < 0 {
				return reply.MakeErrReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg * 1000
			i++ // skip next arg
		} else if arg == "px" { //毫秒单位
			if ttl != unlimitedTTL {
				//ttl has been set
				return reply.MakeSyntaxErrReply()
			}
			// 参数超标不对了
			if i+1 >= len(args) {
				return reply.MakeSyntaxErrReply()
			}
			// 拿到ttl的时间
			ttlArg, err := strconv.ParseInt(string(arg[i+1]), 10, 64)
			if err != nil {
				return reply.MakeSyntaxErrReply()
			}
			if ttlArg < 0 {
				return reply.MakeErrReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg
			i++ // skip next arg
		} else if arg == "PERSIST" {
			if ttl != unlimitedTTL { // PERSIST Cannot be used with EX | PX
				return reply.MakeSyntaxErrReply()
			}
			if i+1 > len(args) {
				return reply.MakeSyntaxErrReply()
			}
			db.Persist(key)
		}
	}
	if len(args) > 1 {
		if ttl != unlimitedTTL { // EX | PX
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expireTime)
			db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
		} else { // PERSIST
			db.Persist(key) // override ttl
			// we convert to persist command to write aof
			db.addAof(utils.ToCmdLine3("persist", args[0]))
		}
	}
	return reply.MakeBulkReply(bytes)
}

// execSet sets string value and time to live to the given key
func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	//aof
	db.addAof(utils.ToCmdLine3("set", args...))
	return reply.MakeOkReply()
}

// execSetNX sets string if not exists
func execSetNX(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	//aof
	db.addAof(utils.ToCmdLine3("setnx", args...))
	return reply.MakeIntReply(int64(result))
}

//
//  @Description: execSetEX sets string and its ttl
//  @param db
//  @param args
//  @return resp.Reply
// SETEX key seconds value
func execSetEX(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[2]
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeSyntaxErrReply()
	}
	if ttlArg <= 0 {
		return reply.MakeErrReply("ERR invalid expire time in setex")
	}
	ttl := ttlArg * 1000
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
	db.Expire(key, expireTime)
	// aof操作
	db.addAof(utils.ToCmdLine3("setex", args...))
	db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
	return reply.MakeOkReply()
}

//
//  @Description: execGetSet sets value of a string-type key and returns its old value
//  @param db
//  @param args
//  @return resp.Reply
// 修改key对应的value，返回原来的key
func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	db.PutEntity(key, &database.DataEntity{
		Data: value,
	})
	//aof
	db.addAof(utils.ToCmdLine3("getset", args...))
	return reply.MakeBulkReply(entity.Data.([]byte))
}

// execStrLen returns len of string value bound to the given key
func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(bytes)))
}

//
// execIncr
//  @Description: INCR key
//  @param db
//  @param args
//  @return resp.Reply
//
func execIncr(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+1, 10)),
		})
		return reply.MakeIntReply(val + 1)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: []byte("1"),
	})
	db.addAof(utils.ToCmdLine3("incr", args...))
	return reply.MakeIntReply(1)
}

//
// execIncrBy
//  @Description:INCRBY key increment
//  @param db
//  @param args
//  @return resp.Reply
//
func execIncrBy(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		// existed value
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+delta, 10)),
		})
		db.addAof(utils.ToCmdLine3("incrby", args...))
		return reply.MakeIntReply(val + delta)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLine3("incrby", args...))
	return reply.MakeIntReply(delta)
}

//
// execIncrByFloat increments the float value of a key by given value
//  @Description: INCRBYFLOAT key increment
//  @param db
//  @param args
//  @return resp.Reply
//
func execIncrByFloat(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := decimal.NewFromString(rawDelta)
	if err != nil {
		return reply.MakeErrReply("ERR value is not a valid float")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := decimal.NewFromString(string(bytes))
		if err != nil {
			return reply.MakeErrReply("ERR value is not a valid float")
		}
		resultBytes := []byte(val.Add(delta).String())
		db.PutEntity(key, &database.DataEntity{
			Data: resultBytes,
		})
		db.addAof(utils.ToCmdLine3("incrbyfloat", args...))
		return reply.MakeBulkReply(resultBytes)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLine3("incrbyfloat", args...))
	return reply.MakeBulkReply(args[1])
}

// execDecr decrements the integer value of a key by one
func execDecr(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-1, 10)),
		})
		db.addAof(utils.ToCmdLine3("decr", args...))
		return reply.MakeIntReply(val - 1)
	}
	entity := &database.DataEntity{
		Data: []byte("-1"),
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine3("decr", args...))
	return reply.MakeIntReply(-1)
}

// execDecrBy decrements the integer value of a key by onedecrement
func execDecrBy(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-delta, 10)),
		})
		db.addAof(utils.ToCmdLine3("decrby", args...))
		return reply.MakeIntReply(val - delta)
	}
	valueStr := strconv.FormatInt(-delta, 10)
	db.PutEntity(key, &database.DataEntity{
		Data: []byte(valueStr),
	})
	db.addAof(utils.ToCmdLine3("decrby", args...))
	return reply.MakeIntReply(-delta)
}
