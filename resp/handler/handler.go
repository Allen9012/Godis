/**
  @author: Allen
  @since: 2023/2/24
  @desc: // 实现响应的Handler
**/
package handler

import (
	"Gedis/database"
	databaseface "Gedis/interface/database"
	"Gedis/lib/logger"
	"Gedis/lib/sync/atomic"
	"Gedis/resp/connection"
	"Gedis/resp/parser"
	"Gedis/resp/reply"
	"context"
	"io"
	"net"
	"strings"
	"sync"
)

var unknownErrReplyBytes = []byte("-ERR unknown\r\n")

type RespHandler struct {
	activeConn sync.Map
	db         databaseface.Database
	closing    atomic.Boolean
}

func MakeHandler() *RespHandler {
	var db databaseface.Database
	db = database.NewDatabase()
	return &RespHandler{
		db: db,
	}
}

// 关闭一个客户端连接
func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	r.db.AfterClientClose(client)
	// 删除map的内容
	r.activeConn.Delete(client)
}

//
// Handle
//  @Description: 实现类似EchoHandler
//  @receiver r
//  @param ctx
//  @param conn
//
func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Get() {
		_ = conn.Close()
	}
	// 获得一个conn
	client := connection.NewConn(conn)
	// todo 先写成空结构体，可以节约空间，后期有需求可以修改
	r.activeConn.Store(client, struct{}{})
	// parser开始工作
	ch := parser.ParseStream(conn)
	// 不断解析ch，死循环
	for payload := range ch {
		// 1. payload有错误
		// 2. payload没有错误
		if payload.Err != nil {
			// 错误类型
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 果断断开连接就可以
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		// exec
		if payload.Data == nil {
			// 啥也没fa
			logger.Info("send nothing: " + client.RemoteAddr().String())
			continue
		}
		multiBulkreply, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := r.db.Exec(client, multiBulkreply.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			// 结果为空， 未知错误
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close 关闭所有连接
func (r *RespHandler) Close() error {
	logger.Info("handler shutting down")
	r.closing.Set(true)
	// 遍历和关闭
	r.activeConn.Range(
		func(key, value any) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true
		},
	)
	r.db.Close()
	return nil
}
