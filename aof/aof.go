/*
*

	@author: Allen
	@since: 2023/2/27
	@desc: //aof

*
*/
package aof

import (
	"github.com/Allen9012/Godis/config"
	databaseface "github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/lib/utils"
	"github.com/Allen9012/Godis/redis/connection"
	"github.com/Allen9012/Godis/redis/parser"
	"github.com/Allen9012/Godis/redis/reply"

	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofBufferSize = 1 << 16
)

const (
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

/* ---slave node version--- */

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

// 命令和db
type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

/* ---old version aof struct--- */

// AofHandler receive msgs from channel and write to AOF file
type AofHandler struct {
	database    databaseface.Database // 用于调用Exec
	aofChan     chan *payload         //该channel将要持久化的命令发送到异步协程
	aofFile     *os.File              //append file 文件描述符
	aofFilename string                //append file 路径
	currentDB   int                   // 上一次写到的db
}

// NewAOFHandler creates a new aof.AofHandler
func NewAOFHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	// 初始化值
	handler.aofFilename = config.Properties.AppendFilename
	handler.database = db
	// 恢复文件，加载aof
	handler.LoadAof()
	// 加载aof,刚启动的时候需要恢复
	aofile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	// 文件写到字段
	handler.aofFile = aofile
	// channel
	handler.aofChan = make(chan *payload, aofBufferSize)
	// 起一个线程handle
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// AddAof send command to aof goroutine through channel
func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {
	// 判断是否打开功能
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}

// handleAof listen aof channel and write into file
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	// 取出payLoad写到文件中
	for payload := range handler.aofChan {
		if payload.dbIndex != handler.currentDB {
			// 插入select语句 *$5select$1[dbindex]
			bytes := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(payload.dbIndex))).ToBytes()
			// 写到文件
			_, err := handler.aofFile.Write(bytes)
			if err != nil {
				logger.Error(err)
				continue
			}
			handler.currentDB = payload.dbIndex
		}
		bytes := reply.MakeMultiBulkReply(payload.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(bytes)
		if err != nil {
			logger.Error(err)
			continue
		}
	}
}

// LoadAof read aof file
//
//	 @Description:	//直接当成用户发送的指令
//	 @receiver server
//		该方法会执行类似Set方法，如果执行，也会调用aof,由于还没有没有初始化aoffunc 所以是一个空方法，需要在makeDB的时候初始化
func (handler *AofHandler) LoadAof() {
	// aof还原（RESP协议编码）
	file, err := os.Open(handler.aofFilename) //open只读
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()
	// File已经实现reader接口
	ch := parser.ParseStream(file)
	//准备一个connection，为了获取dbIndex
	tmpConn := &connection.Connection{}
	for p := range ch {
		// 判断失败方法
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		//我们只需要MultiBulkreply
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("exec multi mulk")
			continue
		}
		// 成功方法
		execReply := handler.database.Exec(tmpConn, r.Args)
		if reply.IsErrorReply(execReply) {
			logger.Error("exec err", execReply.ToBytes())
		}
	}
}

/* ---new version aof struct--- */

// Persister receive msgs from channel and write to AOF file
type Persister struct {
	ctx         context.Context
	cancel      context.CancelFunc
	db          databaseface.DBEngine        // 用于调用Exec
	tmpDBMaker  func() databaseface.DBEngine //  Function type field for generating a temporary database engine instance.
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	aofFsync    string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	currentDB  int
	listeners  map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
}

// NewPersister creates a new aof.Persister
func NewPersister(filename string, db databaseface.DBEngine, fsync string, tmpDBMaker func() databaseface.DBEngine, load bool) (*Persister, error) {
	persister := &Persister{}
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	if load {
		persister.LoadAof(0)
	}
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofBufferSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	go func() {
		// 异步开启监听
		persister.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// LoadAof read aof file, can only be used before Persister.listenCmd started
func (persister *Persister) LoadAof(maxBytes int) {
	// persister.db.Exec may call persister.addAof
	// delete aofChan to prevent loaded commands back into aofChan
	aofChan := persister.aofChan
	// 初始化的时候nil 读写阻塞
	persister.aofChan = nil
	// 执行结束可以aof
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)
	//打开file，开启reader
	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	var reader io.Reader
	if maxBytes > 0 { // 限制大小
		reader = io.LimitReader(file, int64(maxBytes))
	} else { // 默认无限制
		reader = file
	}
	// 复用解析器解析resp
	ch := parser.ParseStream(reader)
	fakeConn := &connection.Connection{} // only used for save dbIndex
	for p := range ch {
		// 判断失败方法
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		//我们只需要MultiBulkreply
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("exec multi mulk")
			continue
		}
		// 执行语句
		ret := persister.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// listenCmd listen aof channel and write into file
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

// fsync every second
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.pausingAof.Lock()
				if err := persister.aofFile.Sync(); err != nil {
					logger.Errorf("fsync failed: %v", err)
				}
				persister.pausingAof.Unlock()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

// Close gracefully stops aof persistence procedure
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished // wait for aof finished
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel()
}

func (persister *Persister) writeAof(p *payload) {
	persister.buffer = persister.buffer[:0] // reuse underlying array
	persister.pausingAof.Lock()             // prevent other goroutines from pausing aof
	defer persister.pausingAof.Unlock()
	// ensure aof is in the right database
	if p.dbIndex != persister.currentDB {
		// select db
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := reply.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	// save command
	data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// RemoveListener removes a listener from aof server, so we can close the listener
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine send command to aof goroutine through channel
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// aofChan will be set as nil temporarily during load aof see Persister.LoadAof
	if persister.aofChan == nil {
		return
	}
	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}
