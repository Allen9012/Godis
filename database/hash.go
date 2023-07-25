package database

import (
	Dict "github.com/Allen9012/Godis/datastruct/dict"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/utils"
	"strconv"
	"strings"
)

func (db *DB) getAsDict(key string) (Dict.Dict, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	dict, ok := entity.Data.(Dict.Dict)
	if !ok {
		return nil, nil
	}
	return dict, nil
}

func (db *DB) getOrInitDict(key string) (dict Dict.Dict, inited bool, errReply protocol.ErrorReply) {
	dict, errReply = db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if dict == nil {
		dict = Dict.MakeSimple()
		db.PutEntity(key, &database.DataEntity{
			Data: dict,
		})
		inited = true
	}
	return dict, inited, nil
}

func init() {
	registerCommand("HSet", execHSet, 4, flagWrite)
	registerCommand("HSetNX", execHSetNX, 4, flagWrite)
	registerCommand("HGet", execHGet, 3, flagReadOnly)
	registerCommand("HExists", execHExists, 3, flagReadOnly)
	registerCommand("HDel", execHDel, -3, flagWrite)
	registerCommand("HLen", execHLen, 2, flagReadOnly)
	registerCommand("HStrlen", execHStrlen, 3, flagReadOnly)
	registerCommand("HMSet", execHMSet, -4, flagWrite)
	registerCommand("HMGet", execHMGet, -3, flagReadOnly)
	registerCommand("HGet", execHGet, -3, flagReadOnly)
	registerCommand("HKeys", execHKeys, 2, flagReadOnly)
	registerCommand("HVals", execHVals, 2, flagReadOnly)
	registerCommand("HGetAll", execHGetAll, 2, flagReadOnly)
	registerCommand("HIncrBy", execHIncrBy, 4, flagWrite)
	registerCommand("HIncrByFloat", execHIncrByFloat, 4, flagWrite)
	registerCommand("HRandField", execHRandField, -2, flagReadOnly)
}

// execHRandField implements HRANDFIELD key [count]
//
//	@Description: HRANDFIELD key [count]
//	@param db
//	@param args
//	@return godis.Reply
func execHRandField(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	count := 1
	withvalues := 0

	if len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hrandfield' command")
	}

	if len(args) == 3 {
		if strings.ToLower(string(args[2])) == "withvalues" {
			withvalues = 1
		} else {
			return protocol.MakeSyntaxErrReply()
		}
	}

	if len(args) >= 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = int(count64)
	}

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	if count > 0 {
		fields := dict.RandomDistinctKeys(count)
		Numfield := len(fields)
		if withvalues == 0 {
			result := make([][]byte, Numfield)
			for i, v := range fields {
				result[i] = []byte(v)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, 2*Numfield)
			for i, v := range fields {
				result[2*i] = []byte(v)
				raw, _ := dict.Get(v)
				result[2*i+1] = raw.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	} else if count < 0 {
		fields := dict.RandomKeys(-count)
		Numfield := len(fields)
		if withvalues == 0 {
			result := make([][]byte, Numfield)
			for i, v := range fields {
				result[i] = []byte(v)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, 2*Numfield)
			for i, v := range fields {
				result[2*i] = []byte(v)
				raw, _ := dict.Get(v)
				result[2*i+1] = raw.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	}

	// 'count' is 0 will reach.
	return &protocol.EmptyMultiBulkReply{}
}

// execHIncrByFloat implements HINCRBYFLOAT key field increment
//
//	@Description: HINCRBYFLOAT key field increment
//	@param db
//	@param args
//	@return godis.Reply
func execHIncrByFloat(db *DB, args [][]byte) godis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("hincrbyfloat")
	}
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(field)
	if !exists {
		dict.Put(field, args[2])
		return protocol.MakeBulkReply(args[2])
	}
	val, err := strconv.ParseFloat(string(value.([]byte)), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR hash value is not a float")
	}
	result := val + delta
	resultBytes := []byte(strconv.FormatFloat(result, 'f', -1, 64))
	dict.Put(field, resultBytes)
	db.addAof(utils.ToCmdLine3("hincrbyfloat", args...))
	return protocol.MakeBulkReply(resultBytes)
}

// execHIncrBy implements HINCRBY key field increment
//
//	@Description: HINCRBY key field increment
//	@param db
//	@param args
//	@return godis.Reply
func execHIncrBy(db *DB, args [][]byte) godis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("hkeys")
	}
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(field)
	if !exists {
		dict.Put(field, args[2])
		db.addAof(utils.ToCmdLine3("hincrby", args...))
		return protocol.MakeBulkReply(args[2])
	}
	val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR hash value is not an integer")
	}
	val += delta
	bytes := []byte(strconv.FormatInt(val, 10))
	dict.Put(field, bytes)
	db.addAof(utils.ToCmdLine3("hincrby", args...))
	return protocol.MakeBulkReply(bytes)
}

// execHGetAll returns all fields and values of the hash stored at key.
//
//	@Description: HGETALL key
//	@param db
//	@param args
//	@return godis.Reply
func execHGetAll(db *DB, args [][]byte) godis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("hkeys")
	}
	key := string(args[0])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := dict.Len()
	result := make([][]byte, size*2)
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		result[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result[:i])
}

// execHVals returns all values in the hash stored at key.
//
//	@Description: HVALS key
//	@param db
//	@param args
//	@return godis.Reply
func execHVals(db *DB, args [][]byte) godis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("hkeys")
	}
	key := string(args[0])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	values := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		values[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(values[:i])
}

// execHKeys returns all field names in the hash stored at key.
//
//	@Description: HKEYS key
//	@param db
//	@param args
//	@return godis.Reply
func execHKeys(db *DB, args [][]byte) godis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("hkeys")
	}
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	fields := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		fields[i] = []byte(key)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(fields[:i])
}

// execHMGet returns the values associated with the specified fields in the hash stored at key.
//
//	@Description: HMGET key field [field ...]
//	@param db
//	@param args
//	@return godis.Reply
func execHMGet(db *DB, args [][]byte) godis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("hmget")
	}
	key := string(args[0])
	size := len(args) - 1
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[i+1])
	}

	// get entity
	result := make([][]byte, size)
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeMultiBulkReply(result)
	}

	for i, field := range fields {
		value, ok := dict.Get(field)
		if !ok {
			result[i] = nil
		} else {
			bytes, _ := value.([]byte)
			result[i] = bytes
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

// execHMSet sets multi fields in hash table
//
//	@Description: HMSET key field value [field value ...]
//	@param db
//	@param args
//	@return godis.Reply
func execHMSet(db *DB, args [][]byte) godis.Reply {
	if len(args) < 3 || len(args)%2 != 1 {
		return protocol.MakeArgNumErrReply("hmset")
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
		values[i] = args[2*i+2]
	}
	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	// put data
	for i, field := range fields {
		value := values[i]
		dict.Put(field, value)
	}
	db.addAof(utils.ToCmdLine3("hmset", args...))
	return protocol.MakeOkReply()
}

// execHStrlen returns the string length of the value associated with field in the hash stored at key
// If the key or the field do not exist, 0 is returned.
//
//	@Description: HSTRLEN key field
//	@param db
//	@param args
//	@return godis.Reply
func execHStrlen(db *DB, args [][]byte) godis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("hstrlen")
	}
	key := string(args[0])
	field := string(args[1])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	raw, exists := dict.Get(field)
	if exists {
		value, _ := raw.([]byte)
		return protocol.MakeIntReply(int64(len(value)))
	}
	return protocol.MakeIntReply(0)
}

// execHLen returns the number of fields contained in the hash stored at key
//
//	@Description: HLEN key
//	@param db
//	@param args
//	@return godis.Reply
func execHLen(db *DB, args [][]byte) godis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("hlen")
	}
	key := string(args[0])
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(dict.Len()))
}

// execHDel deletes one or more hash fields
//
//	@Description: HDEL key field [field ...]
//	@param db
//	@param args
//	@return godis.Reply
func execHDel(db *DB, args [][]byte) godis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("hdel")
	}
	// 解析参数
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}
	deleted := 0
	for _, field := range fields {
		_, result := dict.Remove(field)
		deleted += result
	}
	// 删除完了这个map就删除整个key
	if dict.Len() == 0 {
		db.Remove(key)
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("hdel", args...))
	}

	return protocol.MakeIntReply(int64(deleted))
}

// execHExists determines whether a field exists in a hash table
//
//	@Description: HEXISTS key field
//	@param db
//	@param args
//	@return godis.Reply
func execHExists(db *DB, args [][]byte) godis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("hexists")
	}
	key := string(args[0])
	field := string(args[1])
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	_, exists := dict.Get(field)
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execHGet gets the value of a field in the hash stored at key
//
//	@Description: HGET key field
//	@param db
//	@param args
//	@return godis.Reply
func execHGet(db *DB, args [][]byte) godis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("hget")
	}
	key := string(args[0])
	field := string(args[1])
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeNullBulkReply()
	}
	raw, exists := dict.Get(field)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	value, _ := raw.([]byte)
	return protocol.MakeBulkReply(value)
}

// execHSetNX sets the value of a field in the hash stored at key, only if the field does not exist
//
//	@Description: HSETNX key field value
//	@param db
//	@param args
//	@return godis.Reply
func execHSetNX(db *DB, args [][]byte) godis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("hsetnx")
	}
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	result := dict.PutIfAbsent(field, value)
	if result > 0 {
		db.addAof(utils.ToCmdLine3("hsetnx", args...))
	}
	return protocol.MakeIntReply(int64(result))
}

// execHSet sets the value of a field in the hash stored at key
//
//	@Description: HSET key field value
//	@param db
//	@param args
//	@return godis.Reply
func execHSet(db *DB, args [][]byte) godis.Reply {
	// parse args
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.Put(field, value)
	db.addAof(utils.ToCmdLine3("hset", args...))
	return protocol.MakeIntReply(int64(result))
}
