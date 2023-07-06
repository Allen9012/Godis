package database

/*
	@author: Allen
	@since: 2023/2/27
	@desc: //实现命令的内核
*/

import (
	"fmt"
	"github.com/Allen9012/Godis/aof"
	godis2 "github.com/Allen9012/Godis/config/godis"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/logger"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// StandaloneServer is a set of multiple database set
// implement database.DBEngine
type StandaloneServer struct {
	dbSet     []*atomic.Value // *DB
	persister *aof.Persister

	//// TODO handle publish/subscribe
	//hub *pubsub.Hub
	//// TODO for replication

	//role         int32
	//slaveStatus  *slaveStatus
	//masterStatus *masterStatus
	//
	//// TODO hooks
	//insertCallback database.KeyEventCallback
	//deleteCallback database.KeyEventCallback
}

// NewStandaloneServer creates a godis database with multi database and all other funtions
//
//	@Description: 创建数据库内核
//	@return *StandaloneServer
func NewStandaloneServer() *StandaloneServer {
	server := &StandaloneServer{}
	if godis2.Properties.Databases == 0 {
		godis2.Properties.Databases = 16
	}
	// creat tmp dir
	err := os.MkdirAll(godis2.GetTmpDir(), os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("create tmp dir failed: %v", err))
	}
	// 初始化数据库
	server.dbSet = make([]*atomic.Value, godis2.Properties.Databases)
	// 赋初始值
	for i := range server.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	// 查询是否打开配置
	if godis2.Properties.AppendOnly {
		aofHandler, err := NewPersister(server,
			godis2.Properties.AppendFilename, true, godis2.Properties.AppendFsync)
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofHandler)
	}
	// TODO RDB and slave
	return server
}

// Exec executes command
//	@Description: 执行用户指令，相当于转交给DB处理指令
//	@receiver d
//	@param client
//	@param args eg: set k v | get k | select 2
//	@return redis.Reply
//	Implement database.DB
func (server *StandaloneServer) Exec(c godis.Connection, cmdLine [][]byte) (result godis.Reply) {
	// 核心方法需要recover防止崩溃
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = protocol.MakeUnknowErrReply()
		}
	}()
	// 先处理select
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return execSelect(c, server, cmdLine[1:])
	}
	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(c, cmdLine)
}

// Close graceful shutdown database
// Implement database.DB
func (server *StandaloneServer) Close() {
	if server.persister != nil {
		server.persister.Close()
	}
	//TODO 主从模式需要关闭
}

// AfterClientClose
// Implement database.DB
func (server *StandaloneServer) AfterClientClose(c godis.Connection) {
	//	TODO pubsub 模式需要实现
}

// ExecWithLock executes normal commands, invoker should provide locks
// Implement database.DBEngine
func (server *StandaloneServer) ExecWithLock(conn godis.Connection, cmdLine [][]byte) godis.Reply {
	db, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return db.execWithLock(cmdLine)
}

// ForEach traverses all the keys in the given database
// Implement database.DBEngine
func (server *StandaloneServer) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	server.mustSelectDB(dbIndex).ForEach(cb)
}

// RWLocks lock keys for writing and reading
// Implement database.DBEngine
func (server *StandaloneServer) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
// Implement database.DBEngine
func (server *StandaloneServer) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

// GetDBSize returns keys count and ttl key count
// Implement database.DBEngine
func (server *StandaloneServer) GetDBSize(dbIndex int) (int, int) {
	db := server.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

// GetEntity returns the data entity to the given key
// Implement database.DBEngine
func (server *StandaloneServer) GetEntity(dbIndex int, key string) (*database.DataEntity, bool) {
	return server.mustSelectDB(dbIndex).GetEntity(key)
}

// GetExpiration returns the expiration time of the given key
// Implement database.DBEngine
func (server *StandaloneServer) GetExpiration(dbIndex int, key string) *time.Time {
	raw, ok := server.mustSelectDB(dbIndex).ttlMap.Get(key)
	if !ok {
		return nil
	}
	expireTime, _ := raw.(time.Time)
	return &expireTime
}

//// TODO 实现ExecMulti executes multi commands transaction Atomically and Isolated
//func (server *StandaloneServer) ExecMulti(conn godis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
//	selectedDB, errReply := server.selectDB(conn.GetDBIndex())
//	if errReply != nil {
//		return errReply
//	}
//	return selectedDB.ExecMulti(conn, watching, cmdLines)
//}

//// TODO 实现GetUndoLogs return rollback commands
//func (server *StandaloneServer) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
//	return server.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
//}

// execSelect
//
//	@Description: 用户切换DB
//	@param connection	用户选择的字段存在conn，修改此字段
//	@param database
//	@param args	eg: select 2
//	@return redis.Reply
func execSelect(conn godis.Connection, database *StandaloneServer, args [][]byte) godis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(database.dbSet) {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	conn.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}

// TODO 优化RDB and slave
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

// selectDB returns the database with the given index, or an error if the index is out of range.
func (server *StandaloneServer) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}

// mustSelectDB is like selectDB, but panics if an error occurs.
func (server *StandaloneServer) mustSelectDB(dbIndex int) *DB {
	selectedDB, err := server.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return selectedDB
}
