package database

/*
	@author: Allen
	@since: 2023/4/12
	@desc: //TODO
*/

import (
	HashSet "github.com/Allen9012/Godis/datastruct/set"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/utils"
	"strconv"
)

func init() {
	RegisterCommand("SAdd", execSAdd, -3)
	RegisterCommand("SIsMember", execSIsMember, 3)
	RegisterCommand("SRem", execSRem, -3)
	RegisterCommand("SPop", execSPop, -2)
	RegisterCommand("SCard", execSCard, 2)
	RegisterCommand("SMembers", execSMembers, 2)
	RegisterCommand("SInter", execSInter, -2)
	RegisterCommand("SInterStore", execSInterStore, -3)
	RegisterCommand("SUnion", execSUnion, -2)
	RegisterCommand("SUnionStore", execSUnionStore, -3)
	RegisterCommand("SDiff", execSDiff, -2)
	RegisterCommand("SDiffStore", execSDiffStore, -3)
	RegisterCommand("SRandMember", execSRandMember, -2)
}

func (db *DB) getAsSet(key string) (*HashSet.Set, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(*HashSet.Set)
	if !ok {
		return nil, protocol.MakeWrongTypeErrReply()
	}
	return set, nil
}

func (db *DB) getOrInitSet(key string) (set *HashSet.Set, inited bool, errReply protocol.ErrorReply) {
	set, errReply = db.getAsSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if set == nil {
		set = HashSet.Make()
		db.PutEntity(key, &database.DataEntity{
			Data: set,
		})
		inited = true
	}
	return set, inited, nil
}

// execSRandMember gets random members from set
//
//	@Description: SRANDMEMBER key [count]
//	@param db
//	@param args
//	@return redis.Reply
func execSRandMember(db *DB, args [][]byte) godis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'srandmember' command")
	}
	key := string(args[0])

	// get or init entity
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeNullBulkReply()
	}
	if len(args) == 1 {
		// get a random member, 默认是取出一个
		members := set.RandomMembers(1)
		return protocol.MakeBulkReply([]byte(members[0]))
	}
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	if count > 0 {
		members := set.RandomDistinctMembers(count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return protocol.MakeMultiBulkReply(result)
	} else if count < 0 {
		members := set.RandomMembers(-count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return protocol.MakeMultiBulkReply(result)
	}
	return protocol.MakeEmptyMultiBulkReply()
}

// execSDiffStore subtracts multiple sets and store the result in a key
//
//	@Description: SDIFFSTORE destination key [key ...]
//	@param db
//	@param args
//	@return redis.Reply
func execSDiffStore(db *DB, args [][]byte) godis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	var result *HashSet.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			if i == 0 {
				// early termination
				db.Remove(dest)
				return protocol.MakeIntReply(0)
			}
			continue
		}
		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				// early termination
				db.Remove(dest)
				return protocol.MakeIntReply(0)
			}
		}
	}

	if result == nil {
		// all keys are nil
		db.Remove(dest)
		return protocol.MakeEmptyMultiBulkReply()
	}
	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{
		Data: set,
	})

	db.addAof(utils.ToCmdLine3("sdiffstore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

// execSDiff subtracts multiple sets
//
//	@Description: SDIFF key [key ...]
//	@param db
//	@param args
//	@return redis.Reply
func execSDiff(db *DB, args [][]byte) godis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	var result *HashSet.Set
	for i, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			if i == 0 {
				// early termination
				return protocol.MakeEmptyMultiBulkReply()
			}
			continue
		}
		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Diff(set)
			if result.Len() == 0 {
				// early termination
				return protocol.MakeEmptyMultiBulkReply()
			}
		}
	}

	if result == nil {
		// all keys are nil
		return protocol.MakeEmptyMultiBulkReply()
	}

	ret := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		ret[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(ret)
}

// execSUnionStore adds multiple sets and store the result in a key
//
//	@Description: SUNIONSTORE destination key [key ...]
//	@param db
//	@param args
//	@return redis.Reply
func execSUnionStore(db *DB, args [][]byte) godis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			continue
		}

		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	db.Remove(dest) // clean ttl
	if result == nil {
		// all keys are empty set
		return protocol.MakeEmptyMultiBulkReply()
	}

	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{
		Data: set,
	})

	db.addAof(utils.ToCmdLine3("sunionstore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

// execSUnion adds multiple sets
//
//	@Description: SUNION key [key ...]
//	@param db
//	@param args
//	@return redis.Reply
func execSUnion(db *DB, args [][]byte) godis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			continue
		}

		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Union(set)
		}
	}

	if result == nil {
		// all keys are empty set
		return protocol.MakeEmptyMultiBulkReply()
	}
	ret := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		ret[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(ret)
}

// execSInterStore intersects multiple sets and store the result in a key
//
//	@Description: SINTERSTORE destination key [key ...]
//	@param db
//	@param args
//	@return redis.Reply
func execSInterStore(db *DB, args [][]byte) godis.Reply {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	var result *HashSet.Set
	for _, key := range keys {
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			db.Remove(dest) // clean ttl and old value
			return protocol.MakeIntReply(0)
		}

		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				// early termination
				db.Remove(dest) // clean ttl and old value
				return protocol.MakeIntReply(0)
			}
		}
	}
	set := HashSet.Make(result.ToSlice()...)
	db.PutEntity(dest, &database.DataEntity{
		Data: set,
	})
	db.addAof(utils.ToCmdLine3("sinterscore", args...))
	return protocol.MakeIntReply(int64(set.Len()))
}

// execSInter intersect multiple sets
//
//	@Description: SINTER key [key ...]
//	@param db
//	@param args
//	@return redis.Reply
func execSInter(db *DB, args [][]byte) godis.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	// keys交集到一起
	var result *HashSet.Set
	for _, key := range keys {
		// key -> set
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set == nil {
			return protocol.MakeEmptyMultiBulkReply()
		}
		if result == nil {
			// init
			result = HashSet.Make(set.ToSlice()...)
			if result.Len() == 0 {
				// early termination
				return protocol.MakeEmptyMultiBulkReply()
			}
		} else {
			result = result.Intersect(set)
			if result.Len() == 0 {
				// early termination
				return protocol.MakeEmptyMultiBulkReply()
			}
		}
	}

	// 返回交集
	ret := make([][]byte, result.Len())
	i := 0
	result.ForEach(func(member string) bool {
		ret[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(ret)
}

// execSMembers gets all members in a set
//
//	@Description: SMEMBERS key
//	@param db
//	@param args
//	@return redis.Reply
func execSMembers(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		result[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}

// execSCard gets the number of members in a set
//
//	@Description: SCARD key
//	@param db
//	@param args
//	@return redis.Reply
func execSCard(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	// get or init entity
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(set.Len()))
}

// execSPop removes one or more random members from set
//
//	@Description: SPOP key [count]
//	@param db
//	@param args
//	@return redis.Reply
//	删除返回result
func execSPop(db *DB, args [][]byte) godis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'spop' command")
	}
	key := string(args[0])
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeNullBulkReply()
	}

	count := 1
	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil || count64 <= 0 {
			return protocol.MakeErrReply("ERR value is out of range, must be positive")
		}
		count = int(count64)
	}
	if count > set.Len() {
		count = set.Len()
	}
	// 随机取出count个members
	members := set.RandomDistinctMembers(count)
	result := make([][]byte, len(members))
	for i, v := range members {
		set.Remove(v)
		result[i] = []byte(v)
	}
	if count > 0 {
		db.addAof(utils.ToCmdLine3("spop", args...))
	}
	return protocol.MakeMultiBulkReply(result)
}

// execSRem removes a member from set
//
//	@Description: SREM key member [member ...]
//	@param db
//	@param args
//	@return redis.Reply
func execSRem(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	members := args[1:]

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil {
		return protocol.MakeIntReply(0)
	}

	counter := 0
	for _, member := range members {
		counter += set.Remove(string(member))
	}
	// 删光了
	if set.Len() == 0 {
		db.Remove(key)
	}
	if counter > 0 {
		db.addAof(utils.ToCmdLine3("srem", args...))
	}
	return protocol.MakeIntReply(int64(counter))
}

// execSIsMember checks if the given value is member of set
//
//	@Description: SISMEMBER key member
//	@param db
//	@param args
//	@return redis.Reply
func execSIsMember(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	member := string(args[1])

	// get set
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}

	has := set.Has(member)
	if has {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execSAdd adds members into set
//
//	@Description: SADD key member [member ...]
//	@param db
//	@param args
//	@return redis.Reply
func execSAdd(db *DB, args [][]byte) godis.Reply {
	key := string(args[0])
	members := args[1:]

	// get or init entity
	set, _, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}
	counter := 0
	for _, member := range members {
		counter += set.Add(string(member))
	}
	db.addAof(utils.ToCmdLine3("sadd", args...))
	return protocol.MakeIntReply(int64(counter))
}
