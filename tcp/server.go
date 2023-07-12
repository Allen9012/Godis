package tcp

import (
	"context"
	"fmt"
	"github.com/Allen9012/Godis/interface/tcp"
	"github.com/Allen9012/Godis/lib/logger"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Config 启动server的配置
type Config struct {
	Host       string        `yaml:"host"`
	Port       int           `yaml:"port"`
	MaxConnect uint32        `yaml:"max-connect""`
	Timeout    time.Duration `yaml:"timeout"`
}

// ClientCounter Record the number of clients in the current godis server
var ClientCounter int

// ListenAndServeWithSignal 启动服务
func ListenAndServeWithSignal(config *Config, handler tcp.Handler) error {
	// sigchan发送到closechan
	closeChan := make(chan struct{})
	// 接收信号
	sigChan := make(chan os.Signal)
	// 发送指定信号到信号管道
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 起一个协程，接收关闭信号
	go func() {
		// 异步发送信号给另一个chan
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	// 服务启动地址
	address := config.Host + ":" + strconv.Itoa(config.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	// 顺利开启服务
	logger.Info("Server Listen at ", config.Host, ":", config.Port)
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe 启动
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	errCh := make(chan error, 1)
	defer close(errCh)
	// 先开启一个协程，监听关闭信号执行关闭操作
	go func() {
		select {
		// 没数据就阻塞, 接收到信号就关闭
		case <-closeChan:
			logger.Info("get exit signal")
		case err := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", err.Error()))
		}
		logger.Info("the godis is shutting down, thank you for using godis.")
		// 停止监听，listener.Accept()会立即返回 io.EOF
		_ = listener.Close()
		// 关闭应用层服务器
		_ = handler.Close()
	}()

	// 拿到一个上下文，可以设置初始化
	//ctx, cancel := context.WithCancel(context.Background())
	//// 优化掉panic关闭，两种方式合二为一
	//defer func() {
	//	logger.Info("panic and shutting down gracefully")
	//	// 1. close listening tcp port
	//	if err = listener.Close(); err != nil {
	//		logger.Error(err)
	//	}
	//	_ = server.Close()
	//	// 2. shut down client goroutines (send disconnect msg)
	//	cancel()
	//	// 3. wait for all clients to disconnect
	//	wg.Wait()
	//	logger.Info("See you again. ")
	//}()

	ctx := context.Background()
	var wg sync.WaitGroup

	// 服务一个服务端就+1，服务完就-1
	for true {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			break
		}
		// handle
		logger.Info("accepted link")
		ClientCounter++
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				ClientCounter--
			}()
			// 一个协程连接执行完就wait_group -1
			handler.Handle(ctx, conn)
		}()
	}
	wg.Wait()
}
