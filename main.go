package main

import (
	"github.com/Allen9012/Godis/config"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/resp/handler"
	"github.com/Allen9012/Godis/tcp"
	"os"
)

func main() {
	print(banner)
	// 配置logger屬性
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	// GoRedis的config形式
	cfg, err := config.Setup()
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}

	//配置文件方式或者默认方式启动
	config.Set_godis_config()

	// 业务启动
	err = tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Host: cfg.Host,
			Port: cfg.Port,
		},
		handler.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
