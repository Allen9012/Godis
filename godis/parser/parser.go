package parser

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/logger"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// PayLoad 客户端发送的数据
//
//	PayLoad
//	@Description: 有数据就写到数据字段，反之则写到错误字段
type PayLoad struct {
	// 服务端和客户交互的内容实际是类似的，都是reply
	Data godis.Reply
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

/* 单次解析*/

// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (godis.Reply, error) {
	ch := make(chan *PayLoad)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, protocolError("no protocol")
	}
	return payload.Data, payload.Err
}

// ParseStream 实际可以调用的支持并发的异步的Parser 返回一个只发送的管道
func ParseStream(reader io.Reader) <-chan *PayLoad {
	ch := make(chan *PayLoad)
	go parse0(reader, ch) //	一个用户一个解析器
	return ch             //	返回一个channel，redis核心一直监听ch看看有没有数据产生
}

// parse0 解析器核心
func parse0(rawReader io.Reader, ch chan<- *PayLoad) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(rawReader)
	var state readState
	var err error
	var line []byte
	// 循环解析
	for {
		// read line
		var ioErr bool
		line, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr {
				ch <- &PayLoad{Err: err}
				close(ch)
				return
			}
			// protocol err, reset read state
			ch <- &PayLoad{
				Err: err,
			}
			state = readState{}
			continue
		}
		// 判断多行还是单行模式和
		if !state.readingMultiLine {
			switch line[0] {
			case '*': //eg:*3
				// 利用下面的方法会解析出3，然后修改readingMultiLine改成多行模式
				err = parseMultiBulkHeader(line, &state)
				if err != nil {
					ch <- &PayLoad{Err: protocolError(string(line))}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &PayLoad{
						Data: protocol.MakeEmptyMultiBulkReply(),
					}
					state = readState{}
					continue
				}
			case '$': // 一开始就遇到 $3\r\n
				err = parseBulkHeader(line, &state)
				if err != nil {
					logger.Error(err)
					ch <- &PayLoad{Err: protocolError(string(line))}
					state = readState{}
					continue
				}
				// 空指令
				if state.bulkLen == -1 { // $-1\r\n
					ch <- &PayLoad{Data: protocol.MakeNullBulkReply()}
					state = readState{}
					continue
				}
			case '+': // status reply
				content := strings.TrimSuffix(string(line[1:]), "\r\n")
				ch <- &PayLoad{Data: protocol.MakeStatusReply(content)}
				// TODO RDB action
				//if strings.HasPrefix(content, "FULLRESYNC") {
				//	err = parseRDBBulkString(reader, ch)
				//	if err != nil {
				//		ch <- &Payload{Err: err}
				//		close(ch)
				//		return
				//	}
				//}
				continue
			case '-': // error reply
				content := strings.TrimSuffix(string(line[1:]), "\r\n")
				ch <- &PayLoad{Data: protocol.MakeErrReply(content)}
				continue
			case ':': // int reply
				content := strings.TrimSuffix(string(line[1:]), "\r\n")
				val, err := strconv.ParseInt(content, 10, 64)
				if err != nil {
					logger.Error(err)
					ch <- &PayLoad{Err: protocolError(string(line[1:]))}
				}
				ch <- &PayLoad{Data: protocol.MakeIntReply(val)}
				continue
			default:
				// parse as text protocol
				content := line[:len(line)-2]
				args := bytes.Split(content, []byte{' '})
				ch <- &PayLoad{Data: protocol.MakeMultiBulkReply(args)}
				state = readState{} // reset state
				continue
			}
		} else { // 读多行
			// receive following bulk reply
			err := readBody(line, &state)
			if err != nil {
				ch <- &PayLoad{Err: protocolError(string(line))}
				state = readState{}
				continue
			}
			// if sending finished
			if state.finished() {
				var result godis.Reply
				if state.msgType == '*' {
					result = protocol.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = protocol.MakeBulkReply(state.args[0])
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

// readLine
//
//	@Description: 一次读取一行
//	@param bufreader
//	@param state
//	@return []byte	返回的类似$3\r\n
//	@return bool	是否有IO错误
//	@return error
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	//特殊情况： *3\r\n$3 \r\n$7\r\n[k\r\ney]\r\n$5\r\nvalue\r\n
	//1. \r\n切分行
	//2. 如果有$数字，表示严格读取 \r\n是数据本身不能分行
	var line []byte
	var err error

	if state.bulkLen == 0 { //1. \r\n切分行
		//表示没有预设的个数
		line, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(line) <= 2 || line[len(line)-2] != '\r' {
			// TODO if there are some empty lines within replication traffic, ignore this error
			return nil, false, protocolError(string(line))
		}
	} else { //2. 如果有$数字，表示严格读取 \r\n是数据本身不能分行
		line = make([]byte, state.bulkLen+2) // 多\r\n
		_, err = io.ReadFull(bufReader, line)
		if err != nil {
			return nil, true, err
		}
		if len(line) == 0 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
			return nil, false, protocolError(string(line))
		}
		state.bulkLen = 0 // 读下一行的时候更新bulkLen
	}
	return line, false, nil
}

// parseMultiBulkHeader
//
//	@Description: readLine之后具体取出响应内容，解析多行响应内容
//	@param msg
//	@param state
//	@return error	填充state
func parseMultiBulkHeader(line []byte, state *readState) error {
	//	eg: *3\r\n
	strLen, err := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
	if err != nil {
		logger.Error(err)
		return protocolError("illegal multiBulk string header: " + string(line))
	}
	if strLen < -1 {
		return protocolError("illegal multiBulk string header: " + string(line))
	} else if strLen == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if strLen > 0 {
		state.msgType = line[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(strLen)
		state.args = make([][]byte, 0, strLen)
		return nil
	} else {
		return protocolError("illegal multiBulk string header: " + string(line))
	}
}

// parseBulkHeader
//
//	@Description: 解析单行数据
//	@param msg
//	@param state
//	@return error
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		logger.Error(err)
		return protocolError("illegal bulk string header: " + string(msg))
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
		return protocolError("illegal bulk string header: " + string(msg))
	}
}

////	+Ok\r\n 	-err\r\n 	:5\r\n
////
//// parseSingleLineReply
////
////	@Description: 客户端也可能发送状态 解析状态响应
////	@param msg
////	@return reply	得到reply类型 类似Ok
////	@return err
//func parseSingleLineReply(msg []byte) (godis.Reply, error) {
//	str := strings.TrimSuffix(string(msg), "\r\n")
//	var result godis.Reply
//	switch msg[0] {
//	case '+': // status reply
//		result = reply.MakeStatusReply(str[1:])
//	case '-': // err reply
//		result = reply.MakeErrReply(str[1:])
//	case ':': // int reply
//		val, err := strconv.ParseInt(str[1:], 10, 64)
//		if err != nil {
//			return nil, protocolError(string(msg))
//		}
//		result = reply.MakeIntReply(val)
//	default:
//		// parse as text protocol
//		strs := strings.Split(str, " ")
//		args := make([][]byte, len(strs))
//		for i, s := range strs {
//			args[i] = []byte(s)
//		}
//		result = reply.MakeMultiBulkReply(args)
//	}
//	return result, nil
//}

//		PING\r\n
//
//		readBody
//	 @Description: 实际读出内容和解析 read the non-first lines of multi bulk reply or bulk reply
//	 @param msg	eg:$3
//	 @param state
//	 @return error
func readBody(msg []byte, state *readState) error {
	// 去掉\r\n
	line := msg[0 : len(msg)-2]
	var err error
	//$3
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return protocolError(string(msg))
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

func protocolError(msg string) error {
	return errors.New("protocol error: " + msg)
}

//// there is no CRLF between RDB and following AOF, therefore it needs to be treated differently
//func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
//	header, err := reader.ReadBytes('\n')
//	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
//	if len(header) == 0 {
//		return protocolError("empty header")
//	}
//	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
//	if err != nil || strLen <= 0 {
//		return protocolError("illegal bulk header: " + string(header))
//	}
//	body := make([]byte, strLen)
//	_, err = io.ReadFull(reader, body)
//	if err != nil {
//		return err
//	}
//	ch <- &Payload{
//		Data: protocol.MakeBulkReply(body[:len(body)]),
//	}
//	return nil
//}
