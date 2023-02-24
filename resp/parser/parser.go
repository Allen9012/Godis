package parser

import (
	"Gedis/interface/resp"
	"Gedis/lib/logger"
	"Gedis/resp/reply"
	"bufio"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// PayLoad 客户端发送的数据
//
//  PayLoad
//  @Description: 有数据就写到数据字段，反之则写到错误字段
//
type PayLoad struct {
	// 服务端和客户交互的内容实际是类似的，都是reply
	Data resp.Reply
	Err  error
}

// 解析单行或者多行数据
type readState struct {
	readingMultiLine  bool     // 是否要读多行数据
	expectedArgsCount int      // 参数数量
	msgType           byte     // 数据类型
	args              [][]byte // 具体的传送来的数据
	bulkLen           int64    // 具体的长度 $后面的数字
}

// 是否解析完成
func (s *readState) finished() bool {
	//特殊情况
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// ParseStream 实际可以调用的支持并发的异步的Parser 返回一个只发送的管道
func ParseStream(reader io.Reader) <-chan *PayLoad {
	ch := make(chan *PayLoad)
	go parse0(reader, ch) //	一个用户一个解析器
	return ch             //	返回一个channel，redis核心一直监听ch看看有没有数据产生
}

// parse0 解析器核心
func parse0(reader io.Reader, ch chan<- *PayLoad) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	// 循环解析
	for true {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr {
				ch <- &PayLoad{
					Err: err,
				}
				close(ch)
				return
			}
			ch <- &PayLoad{
				Err: err,
			}
			state = readState{}
			continue
		}
		// 判断多行还是单行模式和
		if !state.readingMultiLine {
			if msg[0] == '*' { //eg:*3
				// 利用下面的方法会解析出3，然后修改readingMultiLine改成多行模式
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &PayLoad{
						Err: err,
					}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &PayLoad{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' { // 一开始就遇到 $3\r\n
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &PayLoad{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}
				// 空指令
				if state.bulkLen == -1 { // $-1\r\n
					ch <- &PayLoad{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else {
				result, err := parseSingleLineReply(msg)
				if err != nil {
					ch <- &PayLoad{
						Data: result,
						Err:  err,
					}
					state = readState{}
					continue
				}
			}
		} else { // 读多行
			err := readBody(msg, &state)
			if err != nil {
				ch <- &PayLoad{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{}
				continue
			}
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &PayLoad{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

//
// readLine
//  @Description: 一次读取一行
//  @param bufreader
//  @param state
//  @return []byte	返回的类似$3\r\n
//  @return bool	是否有IO错误
//  @return error
//
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	//特殊情况： *3\r\n$3\r\n$7\r\n[k\r\ney]\r\n$5\r\nvalue\r\n
	//1. \r\n切分行
	//2. 如果有$数字，表示严格读取 \r\n是数据本身不能分行
	var msg []byte
	var err error

	if state.bulkLen == 0 { //1. \r\n切分行
		//表示没有预设的个数
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		return msg, false, nil
	} else { //2. 如果有$数字，表示严格读取 \r\n是数据本身不能分行
		msg := make([]byte, state.bulkLen+2) // 多两个\r\n
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0 // 读下一行的时候更新buklen
	}
	return msg, false, err
}

//
// parseMultiBulkHeader
//  @Description: readLine之后具体取出响应内容，解析多行响应内容
//  @param msg
//  @param state
//  @return error	填充state
//
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	//	eg: *3\r\n
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

//
// parseBulkHeader
//  @Description: 解析单行数据
//  @param msg
//  @param state
//  @return error
//
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

//	+Ok\r\n 	-err\r\n 	:5\r\n
// parseSingleLineReply
//  @Description: 客户端也可能发送状态 解析状态响应
//  @param msg
//  @return reply	得到reply类型 类似Ok
//  @return err
//
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

//	PING\r\n
//
// 	readBody
//  @Description: 实际读出内容和解析 read the non-first lines of multi bulk reply or bulk reply
//  @param msg	eg:$3
//  @param state
//  @return error
//
func readBody(msg []byte, state *readState) error {
	// 去掉\r\n
	line := msg[0 : len(msg)-2]
	var err error
	//$3
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		// $0\r\n
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{}) // 空参数
			state.bulkLen = 0
		}
	} else {
		// key
		state.args = append(state.args, line)
	}
	return nil
}
