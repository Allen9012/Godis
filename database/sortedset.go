package database

import (
	SortedSet "github.com/Allen9012/Godis/datastruct/sortedset"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/utils"
	"strconv"
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
//	@Description: ZRANK key member [WITHSCORE]
//	@param db
//	@param args
//	@return godis.Reply
func execZRank(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZCount(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRevRank(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZCard(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRange(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRangeByScore(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRevRange(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRevRangeByScore(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZPopMin(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRem(db *DB, args [][]byte) godis.Reply {
	return nil
}

func execZRemRangeByScore(db *DB, args [][]byte) godis.Reply {
	return nil
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
