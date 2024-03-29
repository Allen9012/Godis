package server

import (
	"context"
	"errors"
	"github.com/Allen9012/Godis/cluster"
	"github.com/Allen9012/Godis/config"
	"github.com/Allen9012/Godis/database"
	"github.com/Allen9012/Godis/godis/connection"
	"github.com/Allen9012/Godis/godis/parser"
	"github.com/Allen9012/Godis/godis/protocol"
	databaseface "github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/lib/sync/atomic"
	"io"
	"net"
	"strings"
	"sync"
)

/*
@author: Allen
@since: 2023/2/24
@desc: // A tcp.Handler implements redis protocol
*/

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         databaseface.DB
	closing    atomic.Boolean // refusing new client and new request
}

func MakeHandler() *Handler {
	var db databaseface.DB
	if config.Properties.ClusterEnable {
		db = cluster.MakeCluster()
	} else {
		db = database.NewStandaloneServer()
	}
	return &Handler{
		db: db,
	}
}

// 关闭一个客户端连接
func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	// 删除map的内容
	h.activeConn.Delete(client)
}

// Handle
//
//	@Description: 处理连接
//	@receiver r
//	@param ctx
//	@param conn
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
	}
	// 获得一个conn
	client := connection.NewConn(conn)
	// todo 先写成空结构体，可以节约空间，后期有需求可以修改
	h.activeConn.Store(client, struct{}{})
	// parser开始工作
	ch := parser.ParseStream(conn)
	// 不断解析ch，死循环
	for payload := range ch {
		// 1. payload有错误
		// 2. payload没有错误
		if payload.Err != nil {
			// 错误类型
			if payload.Err == io.EOF || errors.Is(payload.Err, io.ErrUnexpectedEOF) ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 果断断开连接就可以
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		// exec
		if payload.Data == nil {
			// 啥也没发
			logger.Info("send nothing: " + client.RemoteAddr())
			continue
		}
		multiBulkReply, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := h.db.Exec(client, multiBulkReply.Args)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			// 结果为空， 未知错误
			_, _ = client.Write(protocol.MakeUnknowErrReply().ToBytes())
		}
	}
}

// Close 关闭所有连接
func (h *Handler) Close() error {
	logger.Info("server shutting down")
	h.closing.Set(true)
	// 遍历和关闭
	h.activeConn.Range(
		func(key, value any) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true
		},
	)
	h.db.Close()
	return nil
}
