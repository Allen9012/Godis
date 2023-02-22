package tcp

import (
	"context"
	"net"
)

type Handler interface {
	Handle(ctx context.Context, conn net.Conn) //传递超时时间或着等等
	Close() error
}
