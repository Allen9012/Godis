package database

import (
	List "github.com/Allen9012/Godis/datastruct/list"
	"github.com/Allen9012/Godis/godis/reply"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/utils"
	"strconv"
)

/*
	@author: Allen
	@since: 2023/3/12
	@desc: //list
*/
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
		return nil, reply.MakeWrongTypeErrReply()
	}
	return list, nil
}

// getOrInitList
//
//	@Description: 获取value或者创建list
//	@receiver db
//	@param key
//	@return list
//	@return isNew
//	@return errReply
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

// execLRange gets elements of list in given range
//
//	@Description: LRANGE key start stop
//	@param db
//	@param args
//	@return redis.Reply
func execLRange(db *DB, args [][]byte) godis.Reply {
	if len(args) < 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'LRange' command")
	}
	// parse args
	key := string(args[0])
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	start := int(start64)
	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop := int(stop64)
	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeEmptyMultiBulkReply()
	}

	// compute index
	size := list.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return reply.MakeEmptyMultiBulkReply()
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}
	// assert: start in [0, size - 1], stop in [start, size]
	slice := list.Range(start, stop)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		bytes, _ := raw.([]byte)
		result[i] = bytes
	}
	return reply.MakeMultiBulkReply(result)
}

// Sets the list element at index to element.
// @Description: LSET key index element
// @param db
// @param args
// @return resp.Reply
func execLSet(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)
	value := args[2]

	//get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeErrReply("ERR no such key")
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return reply.MakeErrReply("ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return reply.MakeErrReply("ERR index out of range")
	}

	list.Set(index, value)
	db.addAof(utils.ToCmdLine3("lset", args...))
	return reply.MakeOkReply()
}

// LSET key index element
func undoLSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil
	}
	index := int(index64)
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil {
		return nil
	}
	size := list.Len() // assert: size > 0
	if index < -1*size {
		return nil
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return nil
	}
	value, _ := list.Get(index).([]byte)
	return []CmdLine{
		{
			[]byte("LSET"),
			args[0],
			args[1],
			value,
		},
	}
}

// Returns the element at index in the list stored at key.
// @Description: LINDEX key index
// @param db
// @param args
// @return resp.Reply
func execLIndex(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)

	//get entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeNullBulkReply()
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return reply.MakeNullBulkReply()
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return reply.MakeNullBulkReply()
	}

	val, _ := list.Get(index).([]byte)
	return reply.MakeBulkReply(val)
}

// execLLen gets length of list
// @Description: LLEN key
// @param db
// @param args
// @return resp.Reply
func execLLen(db *DB, args [][]byte) godis.Reply {
	// parse args
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}
	size := int64(list.Len())
	return reply.MakeIntReply(size)
}

// Removes the first count occurrences of elements equal to element from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element.
//
//	@Description: LREM key count element
//	@param db
//	@param args
//	@return redis.Reply
func execLRem(db *DB, args [][]byte) godis.Reply {
	// parse args
	key := string(args[0])
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	value := args[2]

	// get data entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}

	var removed int
	if count == 0 {
		removed = list.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		})
	} else if count > 0 {
		removed = list.RemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		}, count)
	} else {
		removed = list.ReverseRemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		}, -count)
	}
	if list.Len() == 0 {
		db.Remove(key)
	}
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("lrem", args...))
	}
	return reply.MakeIntReply(int64(removed))
}

// execRPopLPush pops last element of list-A then insert it to the head of list-B
//
//	@Description: RPOPLPUSH source destination
//	@param db
//	@param args
//	@return redis.Reply
func execRPopLPush(db *DB, args [][]byte) godis.Reply {
	sourceKey := string(args[0])
	destKey := string(args[1])

	// get source entity
	sourceList, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return errReply
	}
	if sourceList == nil {
		return reply.MakeNullBulkReply()
	}

	// get dest entity
	destList, _, errReply := db.getOrInitList(destKey)
	if errReply != nil {
		return errReply
	}

	// pop and push
	val, _ := sourceList.RemoveLast().([]byte)
	destList.Insert(0, val)

	if sourceList.Len() == 0 {
		db.Remove(sourceKey)
	}

	db.addAof(utils.ToCmdLine3("rpoplpush", args...))
	return reply.MakeBulkReply(val)
}

func undoRPopLPush(db *DB, args [][]byte) []CmdLine {
	sourceKey := string(args[0])
	list, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	element, _ := list.Get(list.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
		{
			[]byte("LPOP"),
			args[1],
		},
	}
}

// execRPop
//
//	@Description: RPOP key [count]
//	@param db
//	@param args
//	@return redis.Reply
func execRPop(db *DB, args [][]byte) godis.Reply {
	// parse args
	key := string(args[0])

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeNullBulkReply()
	}

	val, _ := list.RemoveLast().([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	db.addAof(utils.ToCmdLine3("rpop", args...))
	return reply.MakeBulkReply(val)
}

var rPushCmd = []byte("RPUSH")

func undoRPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	element, _ := list.Get(list.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
	}
}

// Removes and returns the first elements of the list stored at key.
// @Description: LPOP key [count]
// @param db
// @param args
// @return resp.Reply
func execLPop(db *DB, args [][]byte) godis.Reply {
	// parse args
	key := string(args[0])

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeNullBulkReply()
	}

	val, _ := list.Remove(0).([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	db.addAof(utils.ToCmdLine3("lpop", args...))
	return reply.MakeBulkReply(val)
}

func undoLPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	element, _ := list.Get(0).([]byte)
	return []CmdLine{
		{
			lPushCmd,
			args[0],
			element,
		},
	}
}

// Inserts specified values at the tail of the list stored at key, only if key already exists and holds a list
// @Description: RPUSHX key element [element ...]
// @param db
// @param args
// @return resp.Reply
func execRPushX(db *DB, args [][]byte) godis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rpushx' command")
	}
	// parse args
	key := string(args[0])
	values := args[1:]

	// get or init entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}
	//put list
	for _, value := range values {
		list.Add(value)
	}
	db.addAof(utils.ToCmdLine3("rpushx", args...))
	return reply.MakeIntReply(int64(list.Len()))
}

var lPushCmd = []byte("LPUSH")

// execRPush
//
//	@Description: RPUSH key element [element ...]
//	@param db
//	@param args
//	@return redis.Reply
func execRPush(db *DB, args [][]byte) godis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	// parse args
	key := string(args[0])
	values := args[1:]

	// get or init entity
	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	//put list
	for _, value := range values {
		list.Add(value)
	}
	db.addAof(utils.ToCmdLine3("rpush", args...))
	return reply.MakeIntReply(int64(list.Len()))
}

// execLPushX inserts element at head of list, only if list exists
//
//	@Description: LPUSHX key element [element ...]
//	@param db
//	@param args
//	@return redis.Reply
func execLPushX(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	values := args[1:]
	// get or init entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}

	//insert
	for _, value := range values {
		list.Insert(0, value)
	}
	db.addAof(utils.ToCmdLine3("lpushx", args...))
	return reply.MakeIntReply(int64(list.Len()))
}

// execLPush
//
//	@Description: LPUSH key element [element ...]
//	@param db
//	@param args
//	@return redis.Reply
func execLPush(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	values := args[1:]

	// get or init
	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	//insert
	for _, value := range values {
		list.Insert(0, value)
	}
	db.addAof(utils.ToCmdLine3("lpush", args...))
	return reply.MakeIntReply(int64(list.Len()))
}

func undoLPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("LPOP", key))
	}
	return cmdLines
}
