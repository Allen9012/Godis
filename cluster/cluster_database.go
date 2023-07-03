/*
*

	@author: Allen
	@since: 2023/2/28
	@desc: // 集群数据库

*
*/
package cluster

import (
	"context"
	"github.com/Allen9012/Godis/config"
	database2 "github.com/Allen9012/Godis/database"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/consistenthash"
	"github.com/Allen9012/Godis/lib/logger"
	pool "github.com/jolestar/go-commons-pool/v2"
	"strings"
)

type ClusterDatabase struct {
	self           string                      //记录自己的地址
	nodes          []string                    // node列表
	peerPicker     *consistenthash.NodeMap     //节点选择器
	peerconnection map[string]*pool.ObjectPool //map保存连接池 节点地址 ： 池
	db             database.DB
}

// MakeClusterDatabase
//
//	 @Description:
//	 @return *ClusterDatabase
//		1. 创建对象，和赋值
//		2. 一致性Hash并添加节点
//	 3. 建立连接池
func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             database2.NewStandaloneServer(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerconnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	// 新建连接池
	for _, peer := range config.Properties.Peers {
		cluster.peerconnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, connectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

// CmdFunc 声明成类型
type CmdFunc func(cluster *ClusterDatabase, c godis.Connection, cmdArgs [][]byte) godis.Reply

var router = makeRouter()

func (c *ClusterDatabase) Exec(client godis.Connection, args [][]byte) (result godis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = protocol.MakeUnknowErrReply()
		}
	}()
	cmdName := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		protocol.MakeErrReply("not supported cmd")
	}
	result = cmdFunc(c, client, args)
	return
}

func (c *ClusterDatabase) Close() {
	c.db.Close()
}

func (c *ClusterDatabase) AfterClientClose(conn godis.Connection) {
	c.db.AfterClientClose(conn)
}
