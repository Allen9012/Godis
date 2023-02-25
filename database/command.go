/**
  @author: Allen
  @since: 2023/2/25
  @desc: //command 执行方法
**/
package database

import "strings"

var cmdTable = make(map[string]*command)

type command struct {
	exector ExecFunc // 每一个command会有一个执行方法，实现执行方法
	arity   int      // 参数数量
}

func RegisterCommand(name string, exector ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		exector: exector,
		arity:   arity,
	}
}
