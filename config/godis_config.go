package config

import (
	"bufio"
	"flag"
	"github.com/Allen9012/Godis/lib/logger"
	"github.com/Allen9012/Godis/lib/utils"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

/* --- GODIS 配置方式 ---*/

const config_file string = "redis.conf"

// Properties holds global config properties
var Properties *ServerProperties

// Mode is the running mode of godis
var (
	ClusterMode    = "cluster"
	StandaloneMode = "standalone"
)

// ServerProperties defines global config properties
type ServerProperties struct {
	// for Public configuration
	RunID             string `cfg:"runid"` // runID always different at every exec.
	Bind              string `cfg:"bind"`
	Port              int    `cfg:"port"`
	Dir               string `cfg:"dir"`
	MaxClients        int    `cfg:"maxclients"`
	RequirePass       string `cfg:"requirepass"`
	Databases         int    `cfg:"databases"`
	AnnounceHost      string `cfg:"announce-host"`
	RDBFilename       string `cfg:"dbfilename"`
	MasterAuth        string `cfg:"masterauth"`
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`
	ClusterEnable     bool   `cfg:"cluster-enable"`
	ClusterAsSeed     bool   `cfg:"cluster-as-seed"`
	ClusterSeed       string `cfg:"cluster-seed"`
	ClusterConfigFile string `cfg:"cluster-config-file"`
	//   AOF
	AppendOnly        bool   `cfg:"appendOnly"` //是否启用AOF
	AppendFilename    string `cfg:"appendFilename"`
	AppendFsync       string `cfg:"appendfsync"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"`
	// for cluster mode configuration
	ClusterEnabled string   `cfg:"cluster-enabled"` // Not used at present.
	Peers          []string `cfg:"peers"`
	Self           string   `cfg:"self"`
	// config file path
	CfPath string `cfg:"cf,omitempty"`
}

type ServerInfo struct {
	StartUpTime time.Time
}

func (p *ServerProperties) AnnounceAddress() string {
	return p.AnnounceHost + ":" + strconv.Itoa(p.Port)
}

var EachTimeServerInfo *ServerInfo

var defaultProperties = &ServerProperties{
	Bind:       "0.0.0.0",
	Port:       9012,
	AppendOnly: false,
	MaxClients: 1000,
	RunID:      utils.RandString(40),
}

func init() {
	// A few stats we don't want to reset: server startup time, and peak mem.
	EachTimeServerInfo = &ServerInfo{
		StartUpTime: time.Now(),
	}

	// default config
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
		RunID:      utils.RandString(40),
	}
	// init flag
	flagInit()
}

/* --- 			flag 启动参数提示				---*/
func flagInit() {
	flag.StringVar(&(Properties.AppendFilename), "config", Properties.AppendFilename, "Appoint a config file: such as redis.conf")
	flag.StringVar(&(Properties.Bind), "bind", Properties.Bind, "Bind host ip: default is 127.0.0.1")
	flag.IntVar(&(Properties.Port), "port", Properties.Port, "Bind a listening port: default is 9012")
	flag.IntVar(&(Properties.MaxClients), "maxClients", Properties.MaxClients, "set the buffer size of channels in PUB/SUB commands. ")
	flag.IntVar(&(Properties.Databases), "Databases", Properties.Databases, "set the number of databases")
	flag.StringVar(&Properties.ClusterConfigFile, "clusterConfigPath", Properties.ClusterConfigFile, "config file path to start cluster mode")
	flag.BoolVar(&Properties.ClusterEnable, "clusterEnable", false, "flag indicates running in cluster mode")
	flag.StringVar(&Properties.Self, "self", "127.0.0.1:9012", "define the address of the current node")
	flag.BoolVar(&Properties.AppendOnly, "appendOnly", true, "flag indicates running in appendOnly mode")
	flag.StringVar(&Properties.AppendFilename, "appendFilename", "appendonly.aof", "define the name of the appendOnly file")
	flag.StringVar(&Properties.AppendFsync, "appendfsync", "everysec", "set the fsync policy for AOF")
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func Set_godis_config() {
	if fileExists(config_file) {
		SetupConfig(config_file)
	} else {
		Properties = defaultProperties
	}

}

// SetupConfig read config file and store properties into Properties
func SetupConfig(configFilename string) {
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}(file)
	Properties = parse(file)
	Properties.RunID = utils.RandString(40)
	// 配置文件路徑
	configFilePath, err := filepath.Abs(configFilename)
	if err != nil {
		logger.Error(err)
		return
	}
	Properties.CfPath = configFilePath
	if Properties.Dir == "" {
		Properties.Dir = "."
	}
	// parse command line flags 命令行配置优先级高
	flag.Parse()
}

// parse config file
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

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}
