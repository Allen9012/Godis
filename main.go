package main

import (
	"Gedis/config"
	"Gedis/lib/logger"
	"Gedis/resp/handler"
	"Gedis/tcp"
	"os"
)

const configFile string = "redis.conf"

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 9012,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	// 第二种config形式
	cfg, err := config.Setup()
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}

	//配置文件方式或者默认方式启动
	if fileExists(configFile) {
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}

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
