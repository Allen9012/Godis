/*
*

	@author: Allen
	@since: 2023/2/25
	@desc: // 测试的echo——base

*
*/
package database

import (
	"github.com/Allen9012/Godis/interface/resp"
	"github.com/Allen9012/Godis/redis/reply"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e EchoDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	return reply.MakeMultiBulkReply(args)
}

func (e EchoDatabase) Close() {

}

func (e EchoDatabase) AfterClientClose(c resp.Connection) {

}
