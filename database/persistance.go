package database

import (
	"github.com/Allen9012/Godis/aof"
	"github.com/Allen9012/Godis/config/godis"
	"github.com/Allen9012/Godis/interface/database"
	"sync/atomic"
)

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/7/2
  @desc:
  @modified by:
**/

func NewPersister(db database.DBEngine, filename string, load bool, fsync string) (*aof.Persister, error) {
	return aof.NewPersister(db, filename, load, fsync, func() database.DBEngine {
		return MakeAuxiliaryServer()
	})
}

func (server *StandaloneServer) AddAof(dbIndex int, cmdLine CmdLine) {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, cmdLine)
	}
}

func (server *StandaloneServer) bindPersister(aofHandler *aof.Persister) {
	server.persister = aofHandler
	// bind SaveCmdLine
	for _, db := range server.dbSet {
		singleDB := db.Load().(*DB)
		singleDB.addAof = func(line CmdLine) {
			if godis.Properties.AppendOnly { // config may be changed during runtime
				server.persister.SaveCmdLine(singleDB.index, line)
			}
		}
	}
}

// MakeAuxiliaryServer create a Server only with basic capabilities for aof rewrite and other usages
func MakeAuxiliaryServer() *StandaloneServer {
	mdb := &StandaloneServer{}
	mdb.dbSet = make([]*atomic.Value, godis.Properties.Databases)
	for i := range mdb.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeBasicDB())
		mdb.dbSet[i] = holder
	}
	return mdb
}
