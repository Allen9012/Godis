package database

/*
	@author: Allen
	@since: 2023/2/25
	@desc: database
*/
import (
	"github.com/Allen9012/Godis/datastruct/dict"
	"github.com/Allen9012/Godis/datastruct/lock"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/lib/timewheel"
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
	index int
	// key -> DataEntity
	data dict.Dict
	// key -> expireTime (time.Time)
	ttlMap dict.Dict // key -> expireTime (time.Time)
	// key -> version(uint32)
	// TODO versionMap is not used now
	versionMap dict.Dict
	// addaof is used to add command to aof
	addAof func(CmdLine)
	// TODO 优化掉 locker
	// dict.Dict will ensure concurrent-safety of its method
	// use this mutex for complicated command only, eg. rpush, incr ...
	locker *lock.Locks
	//// TODO callbacks
	//insertCallback database.KeyEventCallback
	//deleteCallback database.KeyEventCallback
}

// ExecFunc 统一执行方法
// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) godis.Reply

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
		//修改一个bug，增加一个空的实现
		addAof: func(line CmdLine) {},
		// 初始化map 赋值一个SyncMap
		ttlMap:     dict.MakeSyncDict(),
		versionMap: dict.MakeSyncDict(),
		locker:     lock.Make(lockerSize),
	}
	return db
}

// makeBasicDB create DB instance only with basic abilities.
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeSyncDict(),
		ttlMap:     dict.MakeSyncDict(),
		versionMap: dict.MakeSyncDict(),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec executes command within one database
//
//	TODO 优化Exec方法
//	@Description:
//	@receiver db*
//	@param connection
//	@param cmdline
func (db *DB) Exec(connection godis.Connection, cmdLine CmdLine) godis.Reply {
	// 用户发的是什么指令
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command " + cmdName)
	}
	// 校验arity是否合法
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	// SET K V ->K V
	return fun(db, cmdLine[1:])
}

// TODO 优化实现prepare
// func (db *DB) execNormalCommand(cmdLine [][]byte) godis.Reply {
//	cmdName := strings.ToLower(string(cmdLine[0]))
//	cmd, ok := cmdTable[cmdName]
//	if !ok {
//		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
//	}
//	if !validateArity(cmd.arity, cmdLine) {
//		return protocol.MakeArgNumErrReply(cmdName)
//	}
//
//	prepare := cmd.prepare
//	write, read := prepare(cmdLine[1:])
//	db.addVersion(write...)
//	db.RWLocks(write, read)
//	defer db.RWUnLocks(write, read)
//	fun := cmd.executor
//	return fun(db, cmdLine[1:])
//}

// execWithLock executes normal commands, invoker should provide locks
func (db *DB) execWithLock(cmdLine [][]byte) godis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
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

// GetEntity returns DataEntity bind to given key
//
//	@Description: Get
//	@receiver db
//	@param key
//	@return *database.DataEntity
//	@return bool
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	//raw是空接口，需要根据实际类型转化
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
//
//	@Description: Set
//	@receiver db
//	@param key
//	@param entity
//	@return int 存入多少个
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
//
//	@Description:
//	@receiver db
//	@param keys 变长参数
//	@return deleted
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

/* TODO 优化---- Lock Function ----- */

// RWLocks lock keys for writing and reading
// 封装读写锁
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

/* ---- TTL Functions ---- */
func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire sets ttlCmd of key
//
//	@Description: 设置过期时间
//	@receiver db
//	@param key
//	@param expireTime
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

// Persist cancel ttlCmd of key
//
//	@Description: 删除过期时间
//	@receiver db
//	@param key
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	// 调用第三方库删除倒计时
	timewheel.Cancel(taskKey)
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}

		return cb(key, entity, expiration)
	})
}
