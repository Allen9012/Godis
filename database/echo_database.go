/**
  @author: Allen
  @since: 2023/2/25
  @desc: // 测试的echo——base
**/
package database

import (
	"Gedis/interface/resp"
	"Gedis/resp/reply"
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
