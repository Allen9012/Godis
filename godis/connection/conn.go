package connection

/*
	@author: Allen
	@since: 2023/2/24
	@desc: // 协议层和客户端的连接
*/
import (
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

//TODO slave mode
const (
	// flagSlave means this a connection with slave
	flagSlave = uint64(1 << iota)
	// flagSlave means this a connection with master
	flagMaster
	// flagMulti means this connection is within a transaction
	flagMulti
)

type Connection struct {
	conn net.Conn

	// wait until finish sending data, used for graceful shutdown
	sendingData wait.Wait

	// lock while server sending response
	mu    sync.Mutex
	flags uint64

	// subscribing channels
	subs map[string]bool

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// queued commands for `multi`
	queue    [][][]byte
	watching map[string]uint32
	txErrors []error

	// selected db
	selectedDB int
}

var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// NewConn creates Connection instance
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	return c
}

func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *Connection) Close() error {
	// 等待通信结束之后关闭，目的是防止还在传输数据
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	connPool.Put(c)
	return nil
}

// Write sends response to client over tcp connection
//
//	@Description: 给用户写数据
//	@receiver c
//	@param bytes
//	@return error
func (c *Connection) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(bytes)
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(i int) {
	c.selectedDB = i
}
