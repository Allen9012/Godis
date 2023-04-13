/**
  @author: Allen
  @since: 2023/4/12
  @desc: //TODO
**/
package database

import (
	HashSet "Gedis/datastruct/set"
	"Gedis/interface/database"
	"Gedis/interface/resp"
	"Gedis/lib/utils"
	"Gedis/resp/reply"
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

func (db *DB) getAsSet(key string) (*HashSet.Set, reply.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(*HashSet.Set)
	if !ok {
		return nil, reply.MakeWrongTypeErrReply()
	}
	return set, nil
}

func (db *DB) getOrInitSet(key string) (set *HashSet.Set, inited bool, errReply reply.ErrorReply) {
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

func execSRandMember(db *DB, args [][]byte) resp.Reply {

}

func execSDiffStore(db *DB, args [][]byte) resp.Reply {

}

func execSDiff(db *DB, args [][]byte) resp.Reply {

}

func execSUnionStore(db *DB, args [][]byte) resp.Reply {

}

func execSUnion(db *DB, args [][]byte) resp.Reply {

}

func execSInterStore(db *DB, args [][]byte) resp.Reply {

}

func execSInter(db *DB, args [][]byte) resp.Reply {

}

func execSMembers(db *DB, args [][]byte) resp.Reply {

}

func execSCard(db *DB, args [][]byte) resp.Reply {

}

func execSPop(db *DB, args [][]byte) resp.Reply {

}

//
// execSRem
//  @Description: SREM key member [member ...]
//  @param db
//  @param args
//  @return resp.Reply
//
func execSRem(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	members := args[1:]

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}

	if set == nil {
		return reply.MakeIntReply(0)
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
	return reply.MakeIntReply(int64(counter))
}

//
// execSIsMember
//  @Description: SISMEMBER key member
//  @param db
//  @param args
//  @return resp.Reply
//
func execSIsMember(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	member := string(args[1])

	// get set
	set, errorReply := db.getAsSet(key)
	if errorReply != nil {
		return errorReply
	}
	if set == nil {
		return reply.MakeIntReply(0)
	}

	has := set.Has(member)
	if has {
		return reply.MakeIntReply(1)
	}
	return reply.MakeIntReply(0)
}

//
// execSAdd
//  @Description: SADD key member [member ...]
//  @param db
//  @param args
//  @return resp.Reply
//
func execSAdd(db *DB, args [][]byte) resp.Reply {
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
	return reply.MakeIntReply(int64(counter))
}
