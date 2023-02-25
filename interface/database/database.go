/**
  @author: Allen
  @since: 2023/2/24
  @desc: // 代表redis的业务核心
**/
package database

import (
	"Gedis/interface/resp"
)

type CmdLine = [][]byte

type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply //执行操作，回复响应
	Close()                                                //关闭
	AfterClientClose(c resp.Connection)                    //删除后数据清理
}

type DataEntity struct {
	Data interface{}
}
