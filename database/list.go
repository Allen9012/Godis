/**
  @author: Allen
  @since: 2023/3/12
  @desc: //list
**/
package database

import (
	List "Gedis/datastruct/list"
	"Gedis/interface/database"
	"Gedis/interface/resp"
	"Gedis/resp/reply"
)

func init() {
	RegisterCommand("LPush", execLPush, -3)
	RegisterCommand("LPushX", execLPushX, -3)
	RegisterCommand("RPush", execRPush, -3)
	RegisterCommand("RPushX", execRPushX, -3)
	RegisterCommand("LPop", execLPop, 2)
	RegisterCommand("RPop", execRPop, 2)
	RegisterCommand("RPopLPush", execRPopLPush, 3)
	RegisterCommand("LRem", execLRem, 4)
	RegisterCommand("LLen", execLLen, 2)
	RegisterCommand("LIndex", execLIndex, 3)
	RegisterCommand("LSet", execLSet, 4)
	RegisterCommand("LRange", execLRange, 4)
}

/*--- 辅助函数 ---*/
// 返回List类型的value
func (db *DB) getAsList(key string) (List.List, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	list, ok := entity.Data.(List.List)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return list, nil
}

//
// getOrInitList
//  @Description: 获取value或者创建list
//  @receiver db
//  @param key
//  @return list
//  @return isNew
//  @return errReply
//
func (db *DB) getOrInitList(key string) (list List.List, isNew bool, errReply reply.ErrorReply) {
	list, errReply = db.getAsList(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isNew = false
	if list == nil {
		list = List.NewQuickList()
		db.PutEntity(key, &database.DataEntity{
			Data: list,
		})
		isNew = true
	}
	return list, isNew, nil
}

func execLRange(db *DB, args [][]byte) resp.Reply {

}

func execLSet(db *DB, args [][]byte) resp.Reply {

}

func execLIndex(db *DB, args [][]byte) resp.Reply {

}

func execLLen(db *DB, args [][]byte) resp.Reply {

}

func execLRem(db *DB, args [][]byte) resp.Reply {

}

func execRPopLPush(db *DB, args [][]byte) resp.Reply {

}

func execRPop(db *DB, args [][]byte) resp.Reply {

}

func execLPop(db *DB, args [][]byte) resp.Reply {

}

func execRPushX(db *DB, args [][]byte) resp.Reply {

}

func execRPush(db *DB, args [][]byte) resp.Reply {

}

func execLPushX(db *DB, args [][]byte) resp.Reply {

}

//
// execLPush
//  @Description: LPUSH key element [element ...]
//  @param db
//  @param args
//  @return resp.Reply
//
func execLPush(db *DB, args [][]byte) resp.Reply {

}
