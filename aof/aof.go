/**
  @author: Allen
  @since: 2023/2/27
  @desc: //aof
**/
package aof

import (
	"Gedis/config"
	databaseface "Gedis/interface/database"
	"Gedis/lib/logger"
	"Gedis/lib/utils"
	"Gedis/resp/connection"
	"Gedis/resp/parser"
	"Gedis/resp/reply"
	"io"
	"os"
	"strconv"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofBufferSize = 1 << 16
)

// 命令和db
type payload struct {
	cmdLine CmdLine
	dbIndex int
}

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

//
// LoadAof read aof file
//  @Description:	//直接当成用户发送的指令
//  @receiver handler
//	该方法会执行类似Set方法，如果执行，也会调用aof,由于还没有没有初始化aoffunc 所以是一个空方法，需要在makeDB的时候初始化
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
