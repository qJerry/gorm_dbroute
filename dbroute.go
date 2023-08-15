package dbroute

import (
	"database/sql"
	"errors"
	"gorm.io/gorm"
	"sync"
	"time"
)

const (
	Write Operation = "write"
	Read  Operation = "read"

	// Default default route name
	Default ShardingName = "default"

	// Master 主、从
	Master string = "master"
	Slave  string = "slave"
)

type DBRoute struct {
	*gorm.DB
	configs          []Config
	routes           map[string]*route
	global           *route
	prepareStmtStore map[gorm.ConnPool]*gorm.PreparedStmtDB
	compileCallbacks []func(gorm.ConnPool) error
}

type Config struct {
	// default
	Masters map[ShardingName]DialectorConfig
	Slaves  map[ShardingName]DialectorConfig
	// database route policy
	DbPolicy DbPolicy
	// table route policy
	TbPolicy TbPolicy
	// 打印路由信息
	TraceRouteMode bool
	// 对应表
	tables []string
}

// DialectorConfig dialector及属性配置
type DialectorConfig struct {
	Dialector    []gorm.Dialector
	MaxOpen      int
	MaxIdleConns int
	MaxLifetime  time.Duration
	MaxIdleTime  time.Duration
}

func Register(config Config, tables ...string) *DBRoute {
	return (&DBRoute{}).Register(config, tables...)
}

func (dr *DBRoute) Register(config Config, tables ...string) *DBRoute {
	if dr.prepareStmtStore == nil {
		dr.prepareStmtStore = map[gorm.ConnPool]*gorm.PreparedStmtDB{}
	}

	if dr.routes == nil {
		dr.routes = map[string]*route{}
	}

	if config.DbPolicy == nil {
		config.DbPolicy = DbRandomPolicy{}
	}
	if config.TbPolicy == nil {
		config.TbPolicy = TbDefaultPolicy{}
	}

	config.tables = tables
	dr.configs = append(dr.configs, config)
	if dr.DB != nil {
		dr.compileConfig(config)
	}
	return dr
}

func (dr *DBRoute) Name() string {
	return "gorm:db_route"
}

func (dr *DBRoute) Collect(fc func(rows *sql.Rows, err error)) {
	for _, r := range dr.routes {
		for _, ps := range r.masters {
			for _, p := range ps {
				dr.DB.Statement.ConnPool = p
				rows, err := dr.DB.Raw("SHOW STATUS").Rows()
				fc(rows, err)
			}
		}
	}
}

func (dr *DBRoute) Initialize(db *gorm.DB) error {
	dr.DB = db
	dr.registerCallbacks(db)
	return dr.compile()
}

func (dr *DBRoute) compile() error {
	for _, config := range dr.configs {
		if err := dr.compileConfig(config); err != nil {
			return err
		}
	}
	return nil
}

func (dr *DBRoute) compileConfig(config Config) (err error) {
	var (
		connPool = dr.DB.Config.ConnPool
		r        = route{
			dbPolicy:       config.DbPolicy,
			tbPolicy:       config.TbPolicy,
			dbRoute:        dr,
			traceRouteMode: config.TraceRouteMode,
		}
	)

	if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
		connPool = preparedStmtDB.ConnPool
	}

	if len(config.Masters) == 0 {
		r.masters = map[ShardingName][]gorm.ConnPool{Default: {connPool}}
	} else if r.masters, err = dr.convertToConnPool(config.Masters); err != nil {
		return err
	}

	if len(config.Slaves) > 0 {
		if r.slaves, err = dr.convertToConnPool(config.Slaves); err != nil {
			return err
		}
	}

	if len(config.tables) > 0 {
		for _, table := range config.tables {
			dr.routes[table] = &r
		}
	} else if dr.global == nil {
		dr.global = &r
	} else {
		return errors.New("conflicted global resolver")
	}

	for _, fc := range dr.compileCallbacks {
		if err = r.call(fc); err != nil {
			return err
		}
	}

	if config.TraceRouteMode {
		dr.Logger = NewResolverModeLogger(dr.Logger)
	}

	return nil
}

func (dr *DBRoute) convertToConnPool(dialectorsMap map[ShardingName]DialectorConfig) (connPoolMap map[ShardingName][]gorm.ConnPool, err error) {
	connPoolMap = make(map[ShardingName][]gorm.ConnPool)
	config := *dr.DB.Config
	for name, dialectorConfig := range dialectorsMap {
		var connPools []gorm.ConnPool
		for _, dialector := range dialectorConfig.Dialector {
			if db, err := gorm.Open(dialector, &config); err == nil {
				connPool := db.Config.ConnPool
				if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
					connPool = preparedStmtDB.ConnPool
				}

				dr.prepareStmtStore[connPool] = &gorm.PreparedStmtDB{
					ConnPool:    db.Config.ConnPool,
					Stmts:       map[string]*gorm.Stmt{},
					Mux:         &sync.RWMutex{},
					PreparedSQL: make([]string, 0, 100),
				}
				// 配置参数
				SetMaxOpenConns(connPool, dialectorConfig.MaxOpen)
				SetMaxIdleConns(connPool, dialectorConfig.MaxIdleConns)
				SetConnMaxIdleTime(connPool, dialectorConfig.MaxIdleTime)
				SetConnMaxLifetime(connPool, dialectorConfig.MaxLifetime)
				connPools = append(connPools, connPool)
			} else {
				return nil, err
			}
		}
		connPoolMap[name] = connPools
	}
	return connPoolMap, err
}

// routeTb
//
//	@Description: 通过重写sql路由指定分表
//	@param stmt
//	@param sql
//	@return string
func (dr *DBRoute) routeTb(stmt *gorm.Statement) string {
	sql := stmt.SQL.String()
	if len(dr.routes) > 0 {
		if u, ok := stmt.Clauses[usingName].Expression.(using); ok && u.Use != "" {
			if r, ok := dr.routes[u.Use]; ok {
				return r.rewriteSql(stmt, sql)
			}
		}
		if stmt.Table != "" {
			if r, ok := dr.routes[stmt.Table]; ok {
				return r.rewriteSql(stmt, sql)
			}
		}
		if stmt.Schema != nil {
			if r, ok := dr.routes[stmt.Schema.Table]; ok {
				return r.rewriteSql(stmt, sql)
			}
		}
	}
	if dr.global != nil {
		return dr.global.rewriteSql(stmt, sql)
	}
	return sql
}

// @title    路由
// @description   路由指定分库
// @auth jerry
func (dr *DBRoute) routeDb(stmt *gorm.Statement, sql string, op Operation) gorm.ConnPool {
	if len(dr.routes) > 0 {
		if u, ok := stmt.Clauses[usingName].Expression.(using); ok && u.Use != "" {
			if r, ok := dr.routes[u.Use]; ok {
				return r.route(stmt, sql, op)
			}
		}
		if stmt.Table != "" {
			if r, ok := dr.routes[stmt.Table]; ok {
				return r.route(stmt, sql, op)
			}
		}
		if stmt.Schema != nil {
			if r, ok := dr.routes[stmt.Schema.Table]; ok {
				return r.route(stmt, sql, op)
			}
		}
	}
	if dr.global != nil {
		return dr.global.route(stmt, sql, op)
	}
	return stmt.ConnPool
}
