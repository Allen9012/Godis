package config

import (
	"Gedis/lib/logger"
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const (
	PORT                int    = 9012
	MAX_CLIENTS         int    = 10000
	LOCAL_ADDR          string = "127.0.0.1"
	AOF_AUTOSYNC_BYTES  int    = 1024 * 1024 * 10
	DEFULT_AOF_FILENAME        = "appendonly.aof"
)

// ServerProperties defines global config properties
type ServerProperties struct {
	Bind string `cfg:"bind"`
	Port int    `cfg:"port"`

	MaxClients  int      `cfg:"maxclients"`
	RequirePass string   `cfg:"requirepass"`
	Databases   int      `cfg:"databases"`
	Peers       []string `cfg:"peers"`
	Self        string   `cfg:"self"`
	//   AOF
	AppendOnly     bool   `cfg:"appendOnly"` //是否启用AOF
	AppendFilename string `cfg:"appendFilename"`
	// 额外字段
	aofRewriteMinSize  int64  // the AOF file is at least N bytes
	aofCurrentSize     int64  // AOF current size
	aofRewritePerc     int64  // Rewrite AOF if growth > it
	aofRewriteBaseSize int64  // AOF size on latest startup or rewrite
	aofBuf             string // AOF buffer, written before entering the event loop
	aofRewriteChan     chan bool
	aofRewriteBuf      []byte //Hold changes during an AOF rewrite
	aofAutoSyncBytes   int    `cfg:"aofAutoSyncBytes"`
}
type Config struct {
	// flag新增字段
	ConfFile          string
	Port              int
	Host              string
	LogDir            string
	LogLevel          string
	ShardNum          int
	ChanBufferSize    int
	Databases         int
	Others            map[string]any
	ClusterConfigPath string
	IsCluster         bool   `json:"IsCluster"`
	PeerAddrs         string `json:"PeerAddrs"`
	PeerIDs           string `json:"PeerIDs"`
	RaftAddr          string `json:"RaftAddr"`
	NodeID            int    `json:"NodeID"`
	KVPort            int    `json:"KVPort"`
	JoinCluster       bool   `json:"JoinCluster"`
}

var Configures *Config

type CfgError struct {
	message string
}

func (cErr *CfgError) Error() string {
	return cErr.message
}

// Properties holds global config properties
var Properties *ServerProperties

func init() {
	// default config
	Properties = &ServerProperties{
		Bind:             LOCAL_ADDR,
		Port:             PORT,
		AppendOnly:       false,
		aofAutoSyncBytes: AOF_AUTOSYNC_BYTES,
	}
}

func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value
				fieldVal.SetBool(boolValue)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	return config
}

// SetupConfig read config file and store properties into Properties
func SetupConfig(configFilename string) {
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parse(file)
}

/* --- 							---*/
var (
	defaultHost           = "127.0.0.1"
	defaultPort           = 9012
	defaultLogDir         = "./"
	defaultLogLevel       = "info"
	defaultShardNum       = 1024
	defaultChanBufferSize = 10
	configFile            = "./redis.conf"
)

func flagInit(cfg *Config) {
	flag.StringVar(&(cfg.ConfFile), "config", configFile, "Appoint a config file: such as redis.conf")
	flag.StringVar(&(cfg.Host), "host", defaultHost, "Bind host ip: default is 127.0.0.1")
	flag.IntVar(&(cfg.Port), "port", defaultPort, "Bind a listening port: default is 6379")
	flag.StringVar(&(cfg.LogDir), "logdir", defaultLogDir, "Create log directory: default is /tmp")
	flag.StringVar(&(cfg.LogLevel), "loglevel", defaultLogLevel, "Create log level: default is info")
	flag.IntVar(&(cfg.ChanBufferSize), "chanBufSize", defaultChanBufferSize, "set the buffer size of channels in PUB/SUB commands. ")
	// distribution flags
	flag.StringVar(&cfg.ClusterConfigPath, "ClusterConfigPath", "./cluster_config.json", "config file to start cluster mode")
	flag.BoolVar(&cfg.IsCluster, "IsCluster", false, "flag indicates running in cluster mode")
	flag.StringVar(&cfg.PeerAddrs, "PeerAddrs", "http://127.0.0.1:16380", "comma separated cluster peers")
	flag.IntVar(&cfg.NodeID, "NodeID", -1, "node ID")
	flag.IntVar(&cfg.KVPort, "KVPort", 6380, "key-value server port")
	flag.BoolVar(&cfg.JoinCluster, "Join", false, "join an existing cluster")
}

//
// Setup initialize configs and do some validation checking.
// Return configured Config pointer and error.
//  @Description: 尝试直接
//  @param properties
//  @return error
//
func Setup() (*Config, error) {
	cfg := &Config{
		ConfFile:          configFile,
		Host:              defaultHost,
		Port:              defaultPort,
		LogDir:            defaultLogDir,
		LogLevel:          defaultLogLevel,
		ShardNum:          defaultShardNum,
		ChanBufferSize:    defaultChanBufferSize,
		Databases:         16,
		Others:            make(map[string]any),
		ClusterConfigPath: "",
		IsCluster:         false,
		PeerAddrs:         "",
		RaftAddr:          "",
		NodeID:            -1,
		KVPort:            0,
		JoinCluster:       false,
	}
	flagInit(cfg)
	// parse command line flags
	flag.Parse()
	// parse config file & checks
	if cfg.ConfFile != "" {
		if err := cfg.Parse(cfg.ConfFile); err != nil {
			return nil, err
		}
	} else {
		if ip := net.ParseIP(cfg.Host); ip == nil {
			ipErr := &CfgError{
				message: fmt.Sprintf("Given ip address %s is invalid", cfg.Host),
			}
			return nil, ipErr
		}
		if cfg.Port <= 1024 || cfg.Port >= 65535 {
			portErr := &CfgError{
				message: fmt.Sprintf("Listening port should between 1024 and 65535, but %d is given.", cfg.Port),
			}
			return nil, portErr
		}
	}
	// cluster mode
	if cfg.IsCluster {
		if cfg.ClusterConfigPath == "" {
			return nil, errors.New("cluster mode need a cluster config file to start. ")
		}
		err := cfg.ParseConfigJson(cfg.ClusterConfigPath)
		if err != nil {
			return nil, err
		}
	}
	//声明一个全局的方便调用
	Configures = cfg
	return cfg, nil
}
func (cfg *Config) ParseConfigJson(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return errors.New("json config not exist")
	}
	err = json.Unmarshal(data, cfg)
	if cfg.NodeID <= 0 {
		panic("NodeID not set")
	}
	if err != nil {
		return errors.New("Invalid config file fields. ")
	}
	if cfg.RaftAddr == "" {
		cfg.RaftAddr = strings.Split(cfg.PeerAddrs, ",")[cfg.NodeID-1]
	}
	log.Println("RaftAddr = ", cfg.RaftAddr)
	// we only support a single database in cluster mode
	cfg.Databases = 1
	return nil
}

// Parse is used to parse the config file and return error
func (cfg *Config) Parse(cfgFile string) error {
	fl, err := os.Open(cfgFile)
	if err != nil {
		return err
	}

	defer func() {
		err := fl.Close()
		if err != nil {
			fmt.Printf("Close config file error: %s \n", err.Error())
		}
	}()

	reader := bufio.NewReader(fl)
	for {
		line, ioErr := reader.ReadString('\n')
		if ioErr != nil && ioErr != io.EOF {
			return ioErr
		}

		if len(line) > 0 && line[0] == '#' {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			cfgName := strings.ToLower(fields[0])
			switch cfgName {
			case "host":
				if ip := net.ParseIP(fields[1]); ip == nil {
					ipErr := &CfgError{
						message: fmt.Sprintf("Given ip address %s is invalid", cfg.Host),
					}
					return ipErr
				}
				cfg.Host = fields[1]
			case "port":
				port, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				if port <= 1024 || port >= 65535 {
					portErr := &CfgError{
						message: fmt.Sprintf("Listening port should between 1024 and 65535, but %d is given.", port),
					}
					return portErr
				}
				cfg.Port = port
			case "logdir":
				cfg.LogDir = strings.ToLower(fields[1])
			case "loglevel":
				cfg.LogLevel = strings.ToLower(fields[1])
			case "shardnum":
				cfg.ShardNum, err = strconv.Atoi(fields[1])
				if err != nil {
					fmt.Println("ShardNum should be a number. Get: ", fields[1])
					panic(err)
				}
			case "databases":
				cfg.Databases, err = strconv.Atoi(fields[1])
				if err != nil {
					log.Fatal("Databases should be an integer. Get: ", fields[1])
				}
				if cfg.Databases <= 0 {
					log.Fatal("Databases should be an positive integer. Get: ", fields[1])
				}
			default:
				cfg.Others[cfgName] = fields[1]
			}
		}
		if ioErr == io.EOF {
			break
		}
	}
	return nil
}
