package main

import (
	"github.com/Allen9012/Godis/config/godis"
	"github.com/Allen9012/Godis/godis/server"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/tcp"
	"os"
)

func main() {
	print(banner)
	// 配置logger屬性
	if err := logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	}); err != nil {
		logger.Error(err)
		os.Exit(1)
	}
	//// GoRedis的config形式
	//cfg, err := config.Setup()
	//if err != nil {
	//	logger.Error(err)
	//	os.Exit(1)
	//}

	//配置文件方式或者默认方式启动
	godis.Set_godis_config()

	// 业务启动
	err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Host: godis.Properties.Bind,
			Port: godis.Properties.Port,
		},
		server.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
