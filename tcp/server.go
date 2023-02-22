package tcp

import (
	"Gedis/interface/tcp"
	"Gedis/lib/logger"
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Config 启动server的配置
type Config struct {
	Address string
	//todo 最大连接数，超时时间

}

// ListenAndServeWithSignal 使用信号服务
func ListenAndServeWithSignal(config *Config, handler tcp.Handler) error {
	// sigchan发送到closechan
	closeChan := make(chan struct{})
	// 接收信号
	sigChan := make(chan os.Signal)
	// 发送指定信号到信号管道
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		// 异步发送信号给另一个chan
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return err
	}
	logger.Info("start listen")
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe 无信号直接服务
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 没数据就阻塞, 接收到信号关闭
	go func() {
		<-closeChan
		logger.Info("shutting down")
		_ = listener.Close()
		_ = handler.Close()
	}()

	// 如果panic，正常关闭
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	// 拿到一个上下文，可以设置初始化
	ctx := context.Background()
	var wg sync.WaitGroup

	// 服务一个服务端就+1，服务完就-1
	for true {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logger.Info("accepted link")
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
}
