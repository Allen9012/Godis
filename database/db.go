/**
  @author: Allen
  @since: 2023/2/25
  @desc: //DB
**/
package database

import (
	"Gedis/datastruct/dict"
	"Gedis/datastruct/lock"
	"Gedis/interface/database"
	"Gedis/interface/resp"
	"Gedis/lib/logger"
	"Gedis/lib/timewheel"
	"Gedis/resp/reply"
	"strings"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

// DB stores data and execute user's commands
type DB struct {
	index  int
	data   dict.Dict
	addAof func(CmdLine)
	// 增加过期时间功能
	ttlMap dict.Dict // key -> expireTime (time.Time)
	// dict.Dict will ensure concurrent-safety of its method
	// use this mutex for complicated command only, eg. rpush, incr ...
	locker *lock.Locks
}

// ExecFunc 统一执行方法
// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) resp.Reply

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
		//修改一个bug，增加一个空的实现
		addAof: func(line CmdLine) {},
		// 初始化map 赋值一个SyncMap
		ttlMap: dict.MakeSyncDict(),
		locker: lock.Make(lockerSize),
	}
	return db
}

//
// Exec executes command within one database
//  @Description:
//  @receiver db*
//  @param connection
//  @param cmdline
//
func (db *DB) Exec(connection resp.Connection, cmdLine CmdLine) resp.Reply {
	// 用户发的是什么指令
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command " + cmdName)
	}
	// 校验arity是否合法
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.exector
	// SET K V ->K V
	return fun(db, cmdLine[1:])
}

// SET K V -> arity = 3
// EXISTS k1 k2 k3 k4 ... arity = -2 表示可以超过
// 校验是否arity合法
func validateArity(arity int, cmdArgs [][]byte) bool {
	argLen := len(cmdArgs)
	if arity >= 0 {
		return argLen == arity
	}
	// arity < 0 说明参数数量可变
	return argLen >= -arity
}

/* ---- data Access ----- */
// 下面的方法相当于对dict套了一层壳

//
// GetEntity returns DataEntity bind to given key
//  @Description: Get
//  @receiver db
//  @param key
//  @return *database.DataEntity
//  @return bool
//
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	//raw是空接口，需要根据实际类型转化
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

//
// PutEntity a DataEntity into DB
//  @Description: Set
//  @receiver db
//  @param key
//  @param entity
//  @return int 存入多少个
//
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	// 存的时候会自动转化空接口，取的时候需要自己转化
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	db.data.Remove(key)
	// 删除ttl相关
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// Removes the given keys from db
//
// Removes the given keys from db
//  @Description:
//  @receiver db
//  @param keys 变长参数
//  @return deleted
//
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	db.data.Clear()
	// 删除ttl相关
	db.ttlMap.Clear()
	db.locker = lock.Make(lockerSize)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

/* ---- Lock Function ----- */

// RWLocks lock keys for writing and reading
// 封装读写锁
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

//
// Expire sets ttlCmd of key
//  @Description: 设置过期时间
//  @receiver db
//  @param key
//  @param expireTime
//
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	// 指定时间执行操作
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		// 需要锁住所有的keys
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

//
// Persist cancel ttlCmd of key
//  @Description: 删除过期时间
//  @receiver db
//  @param key
//
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	// 调用第三方库删除倒计时
	timewheel.Cancel(taskKey)
}
