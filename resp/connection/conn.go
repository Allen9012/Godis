/*
*

	@author: Allen
	@since: 2023/2/24
	@desc: // 协议层和客户端的连接

*
*/
package connection

import (
	"github.com/Allen9012/Godis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

type Connection struct {
	conn         net.Conn
	waitingReply wait.Wait
	mu           sync.Mutex
	selectedDB   int //	操作的哪一个DB
}

// NewConn 初始化一个conn就可以了
func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
func (c *Connection) Close() error {
	// 等待通信结束之后关闭，目的是防止还在传输数据
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// Write
//
//	@Description: 给用户写数据
//	@receiver c
//	@param bytes
//	@return error
func (c *Connection) Write(bytes []byte) error {
	// 特殊情况
	if len(bytes) == 0 {
		return nil
	}
	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()
	_, err := c.conn.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(i int) {
	c.selectedDB = i
}
