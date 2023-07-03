package database

/**
  @author: Allen
  @since: 2023/2/25
  @desc: command 执行方法
**/
import "strings"

var cmdTable = make(map[string]*command)

type command struct {
	name     string
	executor ExecFunc // 每一个command会有一个执行方法，实现执行方法
	// TODO 优化pre和undo方法
	//// prepare returns related keys command
	//prepare PreFunc
	//// undo generates undo-log before command actually executed, in case the command needs to be rolled back
	//undo UndoFunc
	// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
	// for example: the arity of `get` is 2, `mget` is -2
	arity int
	flags int
	extra *commandExtra
}
type commandExtra struct {
	signs    []string
	firstKey int
	lastKey  int
	keyStep  int
}

// TODO 优化
const flagWrite = 0

const (
	flagReadOnly = 1 << iota
	flagSpecial  // command invoked in Exec
)

func RegisterCommand(name string, exector ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: exector,
		arity:    arity,
	}
}
