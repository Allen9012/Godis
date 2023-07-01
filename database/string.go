package database

/*
	@author: Allen
	@since: 2023/2/26
	@desc: //string
*/
import (
	"github.com/Allen9012/Godis/aof"
	"github.com/Allen9012/Godis/datastruct/bitmap"
	"github.com/Allen9012/Godis/godis/reply"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/lib/utils"
	"github.com/shopspring/decimal"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

// 设置TTL
const unlimitedTTL int64 = 0
const (
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)

// GET
// SET
// SETNX
// GETSET
// STRLEN
// GETEX
// SETEX
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

	RegisterCommand("GetDel", execGetDel, 2)
	// INCR associated
	RegisterCommand("Incr", execIncr, 2)
	RegisterCommand("IncrBy", execIncrBy, 3)
	RegisterCommand("IncrByFloat", execIncrByFloat, 3)
	RegisterCommand("Decr", execDecr, 2)
	RegisterCommand("DecrBy", execDecrBy, 3)
	// APPEND key value
	RegisterCommand("Append", execAppend, 3)
	// BitMap
	RegisterCommand("SetBit", execSetBit, 4)
	RegisterCommand("GetBit", execGetBit, 3)
	RegisterCommand("BitCount", execBitCount, -2)
	RegisterCommand("BitPos", execBitPos, -3)
}

// getAsString
//
//	@Description: key取出bytes字节
//	@receiver db
//	@param key
//	@return []byte
//	@return reply.ErrorReply
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

/* --- BitMap ---*/

// execBitPos
//
//	@Description: BITPOS key bit [start [end [BYTE | BIT]]]
//	@param db
//	@param args
//	@return redis.Reply
//	Return the position of the first bit set to 1 or 0 in a string.
func execBitPos(db *DB, args [][]byte) godis.Reply {
	// 1. 拿到对应的value字节
	// 2. 选择模式
	key := string(args[0])
	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bs == nil {
		return reply.MakeIntReply(-1)
	}
	valStr := string(args[1])
	var v byte
	if valStr == "1" {
		v = 1
	} else if valStr == "0" {
		v = 0
	} else {
		return reply.MakeErrReply("ERR bit is not an integer or out of range")
	}
	byteMode := true
	if len(args) > 4 {
		mode := strings.ToLower(string(args[4]))
		if mode == "bit" {
			byteMode = false
		} else if mode == "byte" {
			byteMode = true
		} else {
			return reply.MakeErrReply("ERR syntax error")
		}
	}
	var size int64
	bm := bitmap.FromBytes(bs)
	if byteMode {
		size = int64(len(*bm))
	} else {
		size = int64(bm.BitSize())
	}
	var beg, end int
	if len(args) > 2 {
		var err2 error
		var startIdx, endIdx int64
		startIdx, err2 = strconv.ParseInt(string(args[2]), 10, 64)
		if err2 != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		endIdx, err2 = strconv.ParseInt(string(args[3]), 10, 64)
		if err2 != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		beg, end = utils.ConvertRange(startIdx, endIdx, size)
		if beg < 0 {
			return reply.MakeIntReply(0)
		}
	}
	if byteMode {
		beg *= 8
		end *= 8
	}
	var offset = int64(-1)
	bm.ForEachBit(int64(beg), int64(end), func(ofs int64, val byte) bool {
		if val == v {
			offset = ofs
			return false
		}
		return true
	})
	return reply.MakeIntReply(offset)
}

// execBitCount
//
//	@Description: BITCOUNT key [start end [BYTE | BIT]]
//	@param db
//	@param args
//	@return redis.Reply
func execBitCount(db *DB, args [][]byte) godis.Reply {
	// 1. 拿到key和对应的模式
	// 2. 拿到起点和终点
	// 3. 拿到size 转化成切片的size
	// 4. 根据模式来计算count返回
	key := string(args[0])
	bs, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bs == nil {
		return reply.MakeIntReply(0)
	}
	byteMode := true
	if len(args) > 3 {
		mode := strings.ToLower(string(args[3]))
		if mode == "bit" {
			byteMode = false
		} else if mode == "byte" {
		} else {
			return reply.MakeSyntaxErrReply()
		}
	}
	var size int64
	bm := bitmap.FromBytes(bs)
	if byteMode {
		size = int64(len(*bm))
	} else {
		size = int64(bm.BitSize())
	}
	var begin, end int
	if len(args) > 1 {
		var err2 error
		var startIdx, endIdx int64
		startIdx, err2 = strconv.ParseInt(string(args[1]), 10, 64)
		if err2 != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		endIdx, err2 = strconv.ParseInt(string(args[2]), 10, 64)
		if err2 != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		begin, end = utils.ConvertRange(startIdx, endIdx, size)
		if begin < 0 {
			return reply.MakeIntReply(0)
		}
	}
	var count int64
	if byteMode {
		bm.ForEachByte(begin, end, func(offset int64, val byte) bool {
			count += int64(bits.OnesCount8(val))
			return true
		})
	} else {
		bm.ForEachBit(int64(begin), int64(end), func(offset int64, val byte) bool {
			if val > 0 {
				count++
			}
			return true
		})
	}
	return reply.MakeIntReply(count)
}

// execGetBit
//
//	@Description: GETBIT key offset
//	@param db
//	@param args
//	@return redis.Reply
func execGetBit(db *DB, args [][]byte) godis.Reply {
	// 1. 拿出key，获取offset
	// 2. 拿出value
	// 4. value转换成bitmap
	// 3. 返回
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR bit offset is not an integer or out of range")
	}
	bs, errorReply := db.getAsString(key)
	if errorReply != nil {
		return errorReply
	}
	if bs == nil {
		return reply.MakeIntReply(0)
	}
	bm := bitmap.FromBytes(bs)
	return reply.MakeIntReply(int64(bm.GetBit(offset)))
}

// execSetBit
//
//	@Description: SETBIT key offset value
//	@param db
//	@param args
//	@return redis.Reply
func execSetBit(db *DB, args [][]byte) godis.Reply {
	// 1. 拿出key，获取偏移，和设置的值
	// 2. key找出value
	// 3. value转换成bitmap
	// 4. 修改
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR bit offset is not an integer or out of range")
	}
	valStr := string(args[2])
	var v byte
	if valStr == "1" {
		v = 1
	} else if valStr == "0" {
		v = 0
	} else {
		return reply.MakeErrReply("ERR bit is not an integer or out of range")
	}
	bs, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	bm := bitmap.FromBytes(bs)
	former := bm.GetBit(offset)
	bm.SetBit(offset, v)
	db.PutEntity(key, &database.DataEntity{Data: bm.ToBytes()})
	db.addAof(utils.ToCmdLine3("setBit", args...))
	return reply.MakeIntReply(int64(former))
}

// execGet returns string value bound to the given key
func execGet(db *DB, args [][]byte) godis.Reply {
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

//		execGetEX Get the value of key and optionally set its expiration
//	 @Description: 注意需要考虑ttl
//	 @param db
//	 @param args		GETEX mykey
//	 @return redis.Reply
//
// EX seconds: 设置指定的过期时间（以秒为单位）。
// PX milliseconds: 设置指定的过期时间（以毫秒为单位）。
// PERSIST: 删除与键关联的任何现有过期时间。
func execGetEX(db *DB, args [][]byte) godis.Reply {
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
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return reply.MakeSyntaxErrReply()
			}
			if ttlArg <= 0 {
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
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return reply.MakeSyntaxErrReply()
			}
			if ttlArg <= 0 {
				return reply.MakeErrReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg
			i++ // skip next arg
		} else if arg == "persist" {
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
//
//	@Description: 增加set的ttl操作
//	@param db
//	@param args
//	@return redis.Reply
//	SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
//	EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
func execSet(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	value := args[1]
	policy := upsertPolicy
	ttl := unlimitedTTL
	// parse options
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToUpper(string(args[i]))
			if arg == "NX" { //insert
				if policy == updatePolicy {
					return reply.MakeSyntaxErrReply()
				}
				policy = insertPolicy
			} else if arg == "XX" { // update policy
				if policy == insertPolicy {
					return reply.MakeSyntaxErrReply()
				}
				policy = updatePolicy
			} else if arg == "EX" { // ttl in seconds
				if ttl != unlimitedTTL {
					// ttl has been set
					return reply.MakeSyntaxErrReply()
				}
				if i+1 >= len(args) {
					return reply.MakeSyntaxErrReply()
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return reply.MakeSyntaxErrReply()
				}
				if ttlArg <= 0 {
					return reply.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg * 1000
				i++ // skip next arg
			} else if arg == "PX" {
				if ttl != unlimitedTTL {
					return reply.MakeSyntaxErrReply()
				}
				if i+1 >= len(args) {
					return reply.MakeSyntaxErrReply()
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return reply.MakeSyntaxErrReply()
				}
				if ttlArg <= 0 {
					return reply.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg
				i++ // skip
			} else {
				return reply.MakeErrReply("ERR invalid expire time in set")
			}
		}
	}
	entity := &database.DataEntity{
		Data: value,
	}
	var result int
	switch policy {
	case upsertPolicy:
		db.PutEntity(key, entity)
		result = 1
	case insertPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExists(key, entity)
	}
	if result > 0 {
		if ttl != unlimitedTTL {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expireTime)
			db.addAof(CmdLine{
				[]byte("SET"),
				args[0],
				args[1],
			})
			db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
		} else {
			db.Persist(key) // override ttl
			db.addAof(utils.ToCmdLine3("set", args...))
		}
	}
	if result > 0 {
		return reply.MakeOkReply()
	}
	return reply.MakeNullBulkReply()
}

// execGetDel Get the value of key and delete the key.
func execGetDel(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])

	old, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if old == nil {
		return reply.MakeNullBulkReply()
	}
	db.Remove(key)

	// We convert to del command to write aof
	db.addAof(utils.ToCmdLine3("del", args...))
	return reply.MakeBulkReply(old)
}

// execSetNX sets string if not exists
func execSetNX(db *DB, args [][]byte) godis.Reply {
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

//	@Description: execSetEX sets string and its ttl
//	@param db
//	@param args
//	@return redis.Reply
//
// SETEX key seconds value
func execSetEX(db *DB, args [][]byte) godis.Reply {
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

//	@Description: execGetSet sets value of a string-type key and returns its old value
//	@param db
//	@param args
//	@return redis.Reply
//
// 修改key对应的value，返回原来的key
func execGetSet(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	value := args[1]
	old, err := db.getAsString(key)
	if err != nil {
		return err
	}
	db.PutEntity(key, &database.DataEntity{
		Data: value,
	})
	db.Persist(key) // override ttl
	//aof
	db.addAof(utils.ToCmdLine3("set", args...))
	if old == nil {
		return reply.MakeNullBulkReply()
	}
	return reply.MakeBulkReply(old)
}

// execStrLen returns len of string value bound to the given key
func execStrLen(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return reply.MakeIntReply(0)
	}
	return reply.MakeIntReply(int64(len(bytes)))
}

// execIncr
//
//	@Description: INCR key
//	@param db
//	@param args
//	@return redis.Reply
func execIncr(db *DB, args [][]byte) godis.Reply {
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

// execIncrBy
//
//	@Description:INCRBY key increment
//	@param db
//	@param args
//	@return redis.Reply
func execIncrBy(db *DB, args [][]byte) godis.Reply {
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

// execIncrByFloat increments the float value of a key by given value
//
//	@Description: INCRBYFLOAT key increment
//	@param db
//	@param args
//	@return redis.Reply
func execIncrByFloat(db *DB, args [][]byte) godis.Reply {
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
func execDecr(db *DB, args [][]byte) godis.Reply {
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
func execDecrBy(db *DB, args [][]byte) godis.Reply {
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

// execAppend
//
//	@Description: APPEND key value
//	@param db
//	@param args
//	@return redis.Reply
func execAppend(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	bytes = append(bytes, args[1]...)
	db.PutEntity(key, &database.DataEntity{Data: bytes})
	db.addAof(utils.ToCmdLine3("append", args...))
	return reply.MakeIntReply(int64(len(bytes)))
}
