/*
*

	@author: Allen
	@since: 2023/2/25
	@desc: // 测试的echo——base

*
*/
package database

import (
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/godis"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e EchoDatabase) Exec(client godis.Connection, args [][]byte) godis.Reply {
	return protocol.MakeMultiBulkReply(args)
}

func (e EchoDatabase) Close() {

}

func (e EchoDatabase) AfterClientClose(c godis.Connection) {

}
