/**
  @author: Allen
  @since: 2023/2/26
  @desc: //string
**/
package database

import (
	"Gedis/interface/database"
	"Gedis/interface/resp"
	"Gedis/lib/logger"
	"Gedis/lib/utils"
	"Gedis/resp/reply"
)

//GET
//SET
//SETNX
//GETSET
//STRLEN

func init() {
	//GET key
	RegisterCommand("Get", execGet, 2)
	//	SET key value (只实现最简单的模式)
	RegisterCommand("Set", execSet, -3)
	//SETNX key value
	RegisterCommand("SetNx", execSetNX, 3)
	//GETSET key value
	RegisterCommand("GetSet", execGetSet, 3)
	//STRLEN key
	RegisterCommand("StrLen", execStrLen, 2)
}

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
		return reply.MakeNullBulkReply()
		logger.Info("execGet can't find value for the key: " + key)
	}
	// 第二个是判断是否转换成功
	bytes, ok := entity.Data.([]byte)
	if !ok {
		//TODO 类型转化错误
		return reply.MakeErrReply(" type transfer error")
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
	db.addAof(utils.ToCmdLine2("set", args...))
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
	db.addAof(utils.ToCmdLine2("setnx", args...))
	return reply.MakeIntReply(int64(result))
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
	db.addAof(utils.ToCmdLine2("getset", args...))
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
