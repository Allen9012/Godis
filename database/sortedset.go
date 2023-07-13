package database

import (
	SortedSet "github.com/Allen9012/Godis/datastruct/sortedset"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/utils"
	"strconv"
	"strings"
)

func init() {
	registerCommand("ZAdd", execZAdd, -4, flagWrite)
	registerCommand("ZScore", execZScore, 3, flagReadOnly)
	registerCommand("ZIncrBy", execZIncrBy, 4, flagWrite)
	registerCommand("ZRank", execZRank, 3, flagReadOnly)
	registerCommand("ZCount", execZCount, 4, flagReadOnly)
	registerCommand("ZRevRank", execZRevRank, 3, flagReadOnly)
	registerCommand("ZCard", execZCard, 2, flagReadOnly)
	registerCommand("ZRange", execZRange, -4, flagReadOnly)
	registerCommand("ZRangeByScore", execZRangeByScore, -4, flagReadOnly)
	registerCommand("ZRevRange", execZRevRange, -4, flagReadOnly)
	registerCommand("ZRevRangeByScore", execZRevRangeByScore, -4, flagReadOnly)
	registerCommand("ZPopMin", execZPopMin, -2, flagWrite)
	registerCommand("ZRem", execZRem, -3, flagWrite)
	registerCommand("ZRemRangeByScore", execZRemRangeByScore, 4, flagWrite)
	registerCommand("ZRemRangeByRank", execZRemRangeByRank, 4, flagWrite)
	registerCommand("ZLexCount", execZLexCount, 4, flagReadOnly)
	registerCommand("ZRangeByLex", execZRangeByLex, -4, flagReadOnly)
	registerCommand("ZRemRangeByLex", execZRemRangeByLex, 4, flagWrite)
	registerCommand("ZRevRangeByLex", execZRevRangeByLex, -4, flagReadOnly)
}

// getAsSortedSet
//
//	@Description: get the SortedSet from db (public method)
//	@receiver db
//	@param key
//	@return *SortedSet.SortedSet
//	@return protocol.ErrorReply
func (db *DB) getAsSortedSet(key string) (*SortedSet.SortedSet, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		// 需要init
		return nil, nil
	}
	// 转化失败，说明类型错误
	sortedSet, ok := entity.Data.(*SortedSet.SortedSet)
	if !ok {
		return nil, protocol.MakeWrongTypeErrReply()
	}
	return sortedSet, nil
}

func (db *DB) getOrInitSortedSet(key string) (sortedSet *SortedSet.SortedSet, isInit bool, errReply protocol.ErrorReply) {
	sortedSet, errReply = db.getAsSortedSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isInit = false
	if sortedSet == nil {
		//	初始化一个
		sortedSet = SortedSet.Make()
		db.PutEntity(key, &database.DataEntity{
			Data: sortedSet,
		})
		isInit = true
	}
	return sortedSet, isInit, nil
}

// execZAdd Adds all the specified members with the specified scores to the sorted set stored at key.
//
//	@Description:  ZADD key [score member]
//	@param db
//	@param args
//	@return godis.Reply
func execZAdd(db *DB, args [][]byte) godis.Reply {
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	elements := make([]*SortedSet.Element, size)
	for i := 0; i < size; i++ {
		scoreValue := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		elements[i] = &SortedSet.Element{
			Score:  score,
			Member: member,
		}
	}
	// get or init entity
	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}
	i := 0
	for _, e := range elements {
		if sortedSet.Add(e.Member, e.Score) {
			i++
		}
	}

	db.addAof(utils.ToCmdLine3("zadd", args...))

	return protocol.MakeIntReply(int64(i))
}

// execZScore gets score of a member in sortedset
//
//	@Description: ZSCORE key member
//	@param db
//	@param args
//	@return godis.Reply
func execZScore(db *DB, args [][]byte) godis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("zscore")
	}
	// parse args
	key := string(args[0])
	member := string(args[1])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeNullBulkReply()
	}
	element, exists := sortedSet.Get(member)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	value := strconv.FormatFloat(element.Score, 'f', -1, 64)
	return protocol.MakeBulkReply([]byte(value))
}

// execZIncrBy increments the score of a member
//
//	@Description: ZINCRBY key increment member
//	@param db
//	@param args
//	@return godis.Reply
func execZIncrBy(db *DB, args [][]byte) godis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("zincrby")
	}
	// parse args
	key := string(args[0])
	incrementValue := string(args[1])
	member := string(args[2])
	delta, err := strconv.ParseFloat(incrementValue, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}
	// get or init entity
	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}
	element, exists := sortedSet.Get(member)
	if !exists {
		sortedSet.Add(member, delta)
		db.addAof(utils.ToCmdLine3("zincrby", args...))
		return protocol.MakeBulkReply(args[1])
	}
	score := element.Score + delta
	sortedSet.Add(member, score)
	bytes := []byte(strconv.FormatFloat(score, 'f', -1, 64))
	db.addAof(utils.ToCmdLine3("zincrby", args...))
	return protocol.MakeBulkReply(bytes)
}

// execZRank gets index of a member in sortedset, ascending order, start from 0
//
//	@Description: ZRANK key member 老版本不支持withscore
//	@param db
//	@param args
//	@return godis.Reply
func execZRank(db *DB, args [][]byte) godis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("zrank")
	}
	// parse args
	key := string(args[0])
	member := string(args[1])

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeNullBulkReply()
	}

	rank := sortedSet.GetRank(member, false)
	if rank < 0 {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeIntReply(rank)
}

// execZRevRank gets index of a member in sortedset, descending order, start from 0
//
//	@Description: ZREVRANK key member
//	@param db
//	@param args
//	@return godis.Reply
func execZRevRank(db *DB, args [][]byte) godis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("zrank")
	}
	// parse args
	key := string(args[0])
	member := string(args[1])

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeNullBulkReply()
	}

	rank := sortedSet.GetRank(member, true)
	if rank < 0 {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeIntReply(rank)
}

// execZCount gets number of members which score within given range
//
//	@Description: ZCOUNT key min max
//	@param db
//	@param args
//	@return godis.Reply
func execZCount(db *DB, args [][]byte) godis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("zcount")
	}
	// parse args
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(sortedSet.RangeCount(min, max))
}

// execZCard  gets number of members in sortedset
//
//	@Description: ZCARD key
//	@param db
//	@param args
//	@return godis.Reply
func execZCard(db *DB, args [][]byte) godis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("zcard")
	}
	// parse args
	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(sortedSet.Len())
}

// execZRange  gets members in range, sort by score in ascending order
//
//	@Description: ZRANGE key start stop
//	@param db
//	@param args
//	@return godis.Reply
func execZRange(db *DB, args [][]byte) godis.Reply {
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeArgNumErrReply("zrange")
	}
	// parse args
	withScores := false
	if len(args) == 4 {
		if string(args[3]) != "WITHSCORES" {
			return protocol.MakeErrReply("ERR syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, false)
}

// execZRevRange gets members in range, sort by score in descending order
//
//	@Description: ZREVRANGE key start stop [WITHSCORES]
//	@param db
//	@param args
//	@return godis.Reply
func execZRevRange(db *DB, args [][]byte) godis.Reply {
	// parse args
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeArgNumErrReply("zrevrange")
	}
	withScores := false
	if len(args) == 4 {
		if string(args[3]) != "WITHSCORES" {
			return protocol.MakeErrReply("syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, true)
}

// execZRangeByScore gets members which score within given range, in ascending order
//
//	@Description: ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
//	@param db
//	@param args
//	@return godis.Reply
func execZRangeByScore(db *DB, args [][]byte) godis.Reply {
	if len(args) < 3 {
		return protocol.MakeArgNumErrReply("zrangebyscore")
	}
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.MakeErrReply("ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, false)
}

// execZRevRangeByScore gets number of members which score within given range, in descending order
//
//	@Description: ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
//	@param db
//	@param args
//	@return godis.Reply
func execZRevRangeByScore(db *DB, args [][]byte) godis.Reply {
	if len(args) < 3 {
		return protocol.MakeArgNumErrReply("zrevrangebyscore")
	}
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.MakeErrReply("ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, true)
}

// execZPopMin Removes and returns up to count members with the lowest scores in the sorted set stored at key.
//
//	@Description: ZPOPMIN key [count]
//	@param db
//	@param args
//	@return godis.Reply
func execZPopMin(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
	}
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	removed := sortedSet.PopMin(count)
	if len(removed) > 0 {
		db.addAof(utils.ToCmdLine3("zpopmin", args...))
	}
	result := make([][]byte, 0, len(removed)*2)
	for _, element := range removed {
		scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
		result = append(result, []byte(element.Member), []byte(scoreStr))
	}
	return protocol.MakeMultiBulkReply(result)
}

// execZRem removes given members
//
//	@Description: ZREM key member [member ...]
//	@param db
//	@param args
//	@return godis.Reply
func execZRem(db *DB, args [][]byte) godis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("zrem")
	}
	// parse args
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			deleted++
		}
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("zrem", args...))
	}
	return protocol.MakeIntReply(deleted)
}

// execZRemRangeByScore removes members with score in given range
//
//	@Description: ZREMRANGEBYSCORE key min max
//	@param db
//	@param args
//	@return godis.Reply
func execZRemRangeByScore(db *DB, args [][]byte) godis.Reply {
	if len(args) != 3 {
		return protocol.MakeArgNumErrReply("zremrangebyscore")
	}
}

func execZRemRangeByRank(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZLexCount(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRangeByLex(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRemRangeByLex(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRevRangeByLex(db *DB, args [][]byte) godis.Reply {
	return nil
}

// range0 gets members in range, sort by score in ascending order
//
//	@Description: 辅助函数
//	@param db
//	@param key
//	@param start
//	@param stop
//	@param withScores
//	@param desc
//	@return godis.Reply
func range0(db *DB, key string, start int64, stop int64, withScores bool, desc bool) godis.Reply {
	//	get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	// mutate index
	size := sortedSet.Len()
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return protocol.MakeEmptyMultiBulkReply()
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}

	// assert: start in [0, size - 1], stop in [start, size]
	slice := sortedSet.RangeByRank(start, stop, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return protocol.MakeMultiBulkReply(result)
	}
	result := make([][]byte, len(slice))
	for i, element := range slice {
		result[i] = []byte(element.Member)
	}
	return protocol.MakeMultiBulkReply(result)
}

// rangeByScore0 param limit: limit < 0 means no limit
//
//	@Description: 辅助函数
//	@param db
//	@param key
//	@param min
//	@param max
//	@param offset
//	@param limit
//	@param withScores
//	@param desc
//	@return redis.Reply
func rangeByScore0(db *DB, key string, min SortedSet.Border, max SortedSet.Border, offset int64, limit int64, withScores bool, desc bool) godis.Reply {
	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	slice := sortedSet.Range(min, max, offset, limit, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return protocol.MakeMultiBulkReply(result)
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result)
}
