package config

import (
	"fmt"
	"gorm.io/gorm/logger"
	"gorm/dbroute"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var (
	C = new(OrmConfig)
	// DbDefaultConfig 默认的db配置
	DbDefaultConfig = DBConfig{}
	// DbMultiConfig 多数据源db配置
	DbMultiConfig = map[string]map[string]map[string]DBConfig{}
	// DbTableAttributeMap 表与库的映射关系
	DbTableAttributeMap = map[string][]string{}
	// DataShardingRuleModelModels 数据分片规则
	DataShardingRuleModelModels []dbroute.DataShardingRuleModel
	once                        sync.Once
)

const (
	DefaultDatabaseName = "default"
)

// DBConfig database config
type DBConfig struct {
	RouteType    string
	DBType       string
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
	MaxLifetime  int
	MaxIdleTime  int
}

// OrmConfig orm global config
type OrmConfig struct {
	Debug         bool
	TablePrefix   string
	SingularTable bool
}

func NewOrmDB() (*gorm.DB, error) {
	ormConfig := C
	cfgMap := DbMultiConfig

	if _, no := cfgMap[DefaultDatabaseName]; !no {
		fmt.Errorf("default db not exist")
		return nil, nil
	}
	// use default config to build db instance
	defaultDialector := openDialector(DbDefaultConfig)
	db, err := gorm.Open(defaultDialector, defaultConfig(ormConfig))
	if err != nil {
		fmt.Errorf("open default dialector errors")
		return nil, err
	}
	if ormConfig.Debug {
		db.Debug()
	}

	// get sharding rule
	shardingTableRuleMap := make(map[string]dbroute.DataShardingRuleModel)
	for _, v := range DataShardingRuleModelModels {
		shardingTableRuleMap[v.Table] = v
	}

	dbRoute := dbroute.DBRoute{}
	// name: datasource name
	for dataSourceName, cfg := range cfgMap {
		relationMap := make(map[string]map[dbroute.ShardingName]dbroute.DialectorConfig)
		// relation: master or slave
		for relation, masterSlaveCfg := range cfg {
			multiDialector := map[dbroute.ShardingName]dbroute.DialectorConfig{}
			// childName: sharding name
			for childName, childCfg := range masterSlaveCfg {
				sharingName := dbroute.ShardingName(dataSourceName)
				if childName != "" {
					sharingName = sharingName + "_" + dbroute.ShardingName(childName)
				}
				dialector := openDialector(childCfg)
				multiDialector[sharingName] = dbroute.DialectorConfig{
					Dialector:    []gorm.Dialector{dialector},
					MaxLifetime:  time.Duration(int64(childCfg.MaxLifetime)),
					MaxIdleTime:  time.Duration(int64(childCfg.MaxIdleTime)),
					MaxOpen:      childCfg.MaxOpenConns,
					MaxIdleConns: childCfg.MaxIdleConns,
				}
			}
			relationMap[relation] = multiDialector
		}

		dbRoute.Register(dbroute.Config{
			Masters:        relationMap[dbroute.Master],
			Slaves:         relationMap[dbroute.Slave],
			DbPolicy:       &dbroute.DbShardingRoutePolicy{DataShardingRuleModelMap: shardingTableRuleMap},
			TbPolicy:       &dbroute.TbShardingRoutePolicy{DataShardingRuleModelMap: shardingTableRuleMap},
			TraceRouteMode: true,
		}, DbTableAttributeMap[dataSourceName]...)
	}
	db.Use(&dbRoute)
	//db.Use(prometheus.New(prometheus.Config{
	//	DBName:          "sl",                     // 使用 `DBName` 作为指标 label
	//	RefreshInterval: 15,                       // 指标刷新频率（默认为 15 秒）
	//	PushAddr:        "monitor pusher address", // 如果配置了 `PushAddr`，则推送指标
	//	StartServer:     true,                     // 启用一个 http 服务来暴露指标
	//	HTTPServerPort:  8080,                     // 配置 http 服务监听端口，默认端口为 8080 （如果您配置了多个，只有第一个 `HTTPServerPort` 会被使用）
	//}))
	return db, err
}

func openDialector(cfg DBConfig) gorm.Dialector {
	var dialector gorm.Dialector
	switch strings.ToLower(cfg.DBType) {
	case "mysql":
		dialector = mysql.Open(cfg.DSN)
	case "postgres":
		dialector = postgres.Open(cfg.DSN)
	default:
	}
	return dialector
}

func defaultConfig(ormConfig *OrmConfig) (config *gorm.Config) {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Info,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)
	return &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   ormConfig.TablePrefix,
			SingularTable: ormConfig.SingularTable,
		},
		Logger:      newLogger,
		PrepareStmt: true,
	}
}
