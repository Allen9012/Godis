/**
  @author: Allen
  @since: 2023/2/27
  @desc: //实现命令的内核
**/
package database

import (
	"Gedis/config"
	"Gedis/interface/resp"
	"Gedis/lib/logger"
	"Gedis/resp/reply"
	"strconv"
	"strings"
)

// Database is a set of multiple database set
type Database struct {
	dbSet []*DB
}

//
// NewDatabase creates a redis database
//  @Description: 创建数据库内核
//  @return *Database
//
func NewDatabase() *Database {
	database := &Database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// 初始化数据库
	database.dbSet = make([]*DB, config.Properties.Databases)
	// 赋初始值
	for i := range database.dbSet {
		newdb := makeDB()
		newdb.index = i
		database.dbSet[i] = newdb
	}
	return database
}

//
// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
//  @Description: 执行用户指令，相当于转交给DB处理指令
//  @receiver d
//  @param client
//  @param args eg: set k v | get k | select 2
//  @return resp.Reply
//
func (d *Database) Exec(client resp.Connection, args [][]byte) resp.Reply {
	// 核心方法需要recover防止崩溃
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	// 先处理select
	cmdName := strings.ToLower(string(args[0]))
	if cmdName == "select" {
		if len(args) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(client, d, args[1:])
	}
	dbIndex := client.GetDBIndex()
	db := d.dbSet[dbIndex]
	return db.Exec(client, args)
}

// Close graceful shutdown database
func (d *Database) Close() {
}

func (d *Database) AfterClientClose(c resp.Connection) {
}

//
// execSelect
//  @Description: 用户切换DB
//  @param connection	用户选择的字段存在conn，修改此字段
//  @param database
//  @param args	eg: select 2
//  @return resp.Reply
//
func execSelect(conn resp.Connection, database *Database, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(database.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	conn.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
