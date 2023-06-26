package tcp

import (
	"bufio"
	"context"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/lib/sync/atomic"
	"github.com/Allen9012/Godis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait //自封waitGroup，增加超时工作
}

func (e *EchoClient) Close() <-chan bool {
	e.Waiting.WaitWithTimeout(10 * time.Second)
	_ = e.Conn.Close()
	return nil
}

// EchoHandler 响应
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if handler.closing.Get() {
		_ = conn.Close()
	}

	client := &EchoClient{
		Conn: conn,
	}
	// 存储到map
	handler.activeConn.Store(client, struct{}{})
	// conn获取读缓冲区
	reader := bufio.NewReader(conn)
	// 服务客户
	for {
		// ReadString 会一直阻塞直到遇到分隔符 '\n'
		// 遇到分隔符后会返回上次遇到分隔符或连接建立后收到的所有数据, 包括分隔符本身
		// 若在遇到分隔符之前遇到异常, ReadString 会返回已收到的数据和错误信息
		msg, err := reader.ReadString('\n')
		if err != nil {
			// 分开err类型，EOF表示读到结尾
			if err == io.EOF {
				logger.Info("Connection close")
				handler.activeConn.Delete(client)
			} else {
				// 发现读取错误
				logger.Warn(err)
			}
			return
		}
		// 增加一个客户端
		client.Waiting.Add(1)
		bytes := []byte(msg)
		// 写回
		_, _ = conn.Write(bytes)
		// 删除客户
		client.Waiting.Done()
	}
}

func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down")
	handler.closing.Set(true)
	// 关掉所有的东西
	handler.activeConn.Range(func(key, value any) bool {
		// 先执行操作，然后
		client := key.(*EchoClient)
		_ = client.Conn.Close()
		// bool指代的是要不要遍历下一个key, true才会继续施加下一个kv
		return true
	})
	return nil
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}
