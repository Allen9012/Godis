package database

import (
	"fmt"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/godis/protocol/asserts"
	"github.com/Allen9012/Godis/lib/utils"
	"math/rand"
	"strconv"
	"testing"
)

func TestSAdd(t *testing.T) {
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	// sadd test
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		result := testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
		asserts.AssertIntReply(t, result, 1)
	}
	// scard test
	result := testDB.Exec(nil, utils.ToCmdLine("scard", key))
	asserts.AssertIntReply(t, result, size)
	//	test SIsMember
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		result := testDB.Exec(nil, utils.ToCmdLine("sismember", key, member))
		asserts.AssertIntReply(t, result, 1)
	}

	//	test members
	result = testDB.Exec(nil, utils.ToCmdLine("smembers", key))
	multiBulk, ok := result.(*protocol.MultiBulkReply)
	if !ok {
		t.Error(fmt.Sprintf("expected bulk protocol, actually %s", result.ToBytes()))
		return
	}
	if len(multiBulk.Args) != size {
		t.Error(fmt.Sprintf("expected %d members, actually %d", size, len(multiBulk.Args)))
		return
	}
}

func TestSRem(t *testing.T) {
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
	}
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		testDB.Exec(nil, utils.ToCmdLine("srem", key, member))
		result := testDB.Exec(nil, utils.ToCmdLine("SIsMember", key, member))
		asserts.AssertIntReply(t, result, 0)
	}
}

func TestSPop(t *testing.T) {
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		member := strconv.Itoa(i)
		testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
	}
	result := testDB.Exec(nil, utils.ToCmdLine("spop", key))
	asserts.AssertMultiBulkReplySize(t, result, 1)
	currentSize := size - 1
	for currentSize > 0 {
		// 随机删减一个k-v
		count := rand.Intn(currentSize) + 1
		resultSpop := testDB.Exec(nil, utils.ToCmdLine("spop", key, strconv.FormatInt(int64(count), 10)))
		multiBulk, ok := resultSpop.(*protocol.MultiBulkReply)
		if !ok {
			t.Error(fmt.Sprintf("expected bulk protocol, actually %s", resultSpop.ToBytes()))
			return
		}
		// 返回的是删除的大小
		removedSize := len(multiBulk.Args)
		for _, arg := range multiBulk.Args {
			// 判断剩余中已经删除，不在集合中
			resultSIsMember := testDB.Exec(nil, utils.ToCmdLine("SIsMember", key, string(arg)))
			asserts.AssertIntReply(t, resultSIsMember, 0)
		}
		currentSize -= removedSize
		// 获取当前集合大小判断计算后是否等于currentSize
		resultSCard := testDB.Exec(nil, utils.ToCmdLine("SCard", key))
		asserts.AssertIntReply(t, resultSCard, currentSize)
	}
}

// TestSInter
//
//	@Description:
//	@param t
//	1. 检查交集数量正确
//	2. 测试 极端条件
func TestSInter(t *testing.T) {
	testDB.Flush()
	size := 100
	step := 10
	setNum := 4
	keys := make([]string, 0)
	start := 0
	// 制造每个大小为100 的4个集合, 用step使之重和
	for i := 0; i < setNum; i++ {
		key := utils.RandString(10) + strconv.Itoa(i)
		keys = append(keys, key)
		// 模拟插入size大小的数据
		for j := start; j < start+size; j++ {
			member := strconv.Itoa(j)
			testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
		}
		start += step
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("sinter", keys...))
	asserts.AssertMultiBulkReplySize(t, result, size-step*(setNum-1))

	// test empty set
	testDB.Flush()
	// 制造一个空交集
	key0 := utils.RandString(10)
	testDB.Remove(key0)
	// 制造两个没有交集的集合
	key1 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key1, "a", "b"))
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key2, "1", "2"))
	result = testDB.Exec(nil, utils.ToCmdLine("sinter", key0, key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("sinter", key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	// intersact and store
	result = testDB.Exec(nil, utils.ToCmdLine("sinterstore", utils.RandString(10), key0, key1, key2))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("sinterstore", utils.RandString(10), key1, key2))
	asserts.AssertIntReply(t, result, 0)
}

func TestSUnion(t *testing.T) {
	testDB.Flush()
	size := 100
	step := 10
	setNum := 4
	keys := make([]string, 0)
	start := 0
	// 制造每个大小为100 的4个集合, 用step使之重和
	for i := 0; i < setNum; i++ {
		key := utils.RandString(10) + strconv.Itoa(i)
		keys = append(keys, key)
		// 模拟插入size大小的数据
		for j := start; j < start+size; j++ {
			member := strconv.Itoa(j)
			testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
		}
		start += step
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("sunion", keys...))
	asserts.AssertMultiBulkReplySize(t, result, size+step*(setNum-1))
	// 通过Union使得destination 放在keysWithDest[0],并且返回值是全集
	destKey := utils.RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = testDB.Exec(nil, utils.ToCmdLine2("sunionstore", keysWithDest...))
	asserts.AssertIntReply(t, result, size+step*(setNum-1))
}

func TestSDiff(t *testing.T) {
	testDB.Flush()
	size := 100
	step := 20
	setNum := 3
	keys := make([]string, 0)
	start := 0
	// 制造每个大小为100 的4个集合, 用step使之重和
	for i := 0; i < setNum; i++ {
		key := utils.RandString(10)
		keys = append(keys, key)
		// 模拟插入size大小的数据
		for j := start; j < start+size; j++ {
			member := strconv.Itoa(j)
			testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
		}
		start += step
	}

	result := testDB.Exec(nil, utils.ToCmdLine2("sdiff", keys...))
	asserts.AssertMultiBulkReplySize(t, result, step)

	destKey := utils.RandString(10)
	keysWithDest := []string{destKey}
	keysWithDest = append(keysWithDest, keys...)
	result = testDB.Exec(nil, utils.ToCmdLine2("SDiffStore", keysWithDest...))
	asserts.AssertIntReply(t, result, step)

	// test empty set
	testDB.Flush()
	key0 := utils.RandString(10)
	testDB.Remove(key0)
	key1 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key1, "a", "b"))
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key2, "a", "b"))
	result = testDB.Exec(nil, utils.ToCmdLine("sdiff", key0, key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("sdiff", key1, key2))
	asserts.AssertMultiBulkReplySize(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("SDiffStore", utils.RandString(10), key0, key1, key2))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("SDiffStore", utils.RandString(10), key1, key2))
	asserts.AssertIntReply(t, result, 0)
}

func TestSRandMember(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	for j := 0; j < 100; j++ {
		member := strconv.Itoa(j)
		testDB.Exec(nil, utils.ToCmdLine("sadd", key, member))
	}
	result := testDB.Exec(nil, utils.ToCmdLine("SRandMember", key))
	br, ok := result.(*protocol.BulkReply)
	if !ok && len(br.Arg) > 0 {
		t.Error(fmt.Sprintf("expected bulk protocol, actually %s", result.ToBytes()))
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "10"))
	asserts.AssertMultiBulkReplySize(t, result, 10)
	multiBulk, ok := result.(*protocol.MultiBulkReply)
	if !ok {
		t.Error(fmt.Sprintf("expected bulk protocol, actually %s", result.ToBytes()))
		return
	}
	m := make(map[string]struct{})
	for _, arg := range multiBulk.Args {
		m[string(arg)] = struct{}{}
	}
	if len(m) != 10 {
		t.Error(fmt.Sprintf("expected 10 members, actually %d", len(m)))
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "110"))
	asserts.AssertMultiBulkReplySize(t, result, 100)

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "-10"))
	asserts.AssertMultiBulkReplySize(t, result, 10)

	result = testDB.Exec(nil, utils.ToCmdLine("SRandMember", key, "-110"))
	asserts.AssertMultiBulkReplySize(t, result, 110)
}
