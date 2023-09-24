package aof

/*
	@author: Allen
	@since: 2023/4/11
	@desc: //rewrite
*/

import (
	"github.com/Allen9012/Godis/config"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/database"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/lib/utils"
	"io"
	"os"
	"strconv"
	"time"
)

// 重写AOF
func (persister *Persister) newRewriteHandler() *Persister {
	p := &Persister{}
	p.aofFilename = persister.aofFilename
	p.db = persister.tmpDBMaker()
	return p
}

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// Rewrite carries out AOF rewrite
func (persister *Persister) Rewrite() error {
	ctx, err := persister.StartRewrite()
	if err != nil {
		return err
	}
	err = persister.DoRewrite(ctx)
	if err != nil {
		return err
	}
	persister.FinishRewrite(ctx)
	return nil
}

// DoRewrite actually rewrite aof file
// makes DoRewrite public for testing only, please use Rewrite instead
func (persister *Persister) DoRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile

	// load aof tmpFile
	tmpAof := persister.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))

	// rewrite aof tmpFile
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		//dump db
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

/*开始和结束的时候需要注意*/

// StartRewrite prepares rewrite procedure
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// 暂停 aof 写入， 数据会在 aofChan 中暂时堆积
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	// 调用 fsync 将缓冲区中的数据落盘，防止 aof 文件不完整造成错误
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, _ := os.Stat(persister.aofFilename)
	fileSize := fileInfo.Size()

	// create tmp file
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: fileSize,
		dbIdx:    persister.currentDB,
	}, nil
}

// FinishRewrite finish rewrite procedure
func (persister *Persister) FinishRewrite(ctx *RewriteCtx) {
	persister.pausingAof.Lock() // pausing aof
	defer persister.pausingAof.Unlock()

	tmpFile := ctx.tmpFile
	// write commands executed during rewriting to tmp file
	errOccurs := func() bool {
		/* read write commands executed during rewriting */
		src, err := os.Open(persister.aofFilename)
		if err != nil {
			logger.Error("open aofFilename failed: " + err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tmpFile.Close()
		}()

		_, err = src.Seek(ctx.fileSize, 0)
		if err != nil {
			logger.Error("seek failed: " + err.Error())
			return true
		}
		// 写入一条 Select 命令，使 tmpAof 选中重写开始时刻线上 aof 文件选中的数据库
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
		_, err = tmpFile.Write(data)
		if err != nil {
			logger.Error("tmp file rewrite failed: " + err.Error())
			return true
		}
		// 对齐数据库后就可以把重写过程中产生的数据复制到 tmpAof 文件了
		_, err = io.Copy(tmpFile, src)
		if err != nil {
			logger.Error("copy aof filed failed: " + err.Error())
			return true
		}
		return false
	}()
	if errOccurs {
		return
	}

	// 使用 mv 命令用 tmpAof 代替线上 aof 文件
	_ = persister.aofFile.Close()
	if err := os.Rename(tmpFile.Name(), persister.aofFilename); err != nil {
		logger.Warn(err)
	}
	// 重新打开线上 aof
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	// write select command again to resume aof file selected db
	// it should have the same db index with  persister.currentDB
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
