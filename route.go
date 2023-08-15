package dbroute

import (
	"gorm.io/gorm"
)

type route struct {
	masters               map[ShardingName][]gorm.ConnPool
	slaves                map[ShardingName][]gorm.ConnPool
	dbPolicy              DbPolicy
	tbPolicy              TbPolicy
	dbRoute               *DBRoute
	traceRouteMode        bool
	DataShardingRuleModel DataShardingRuleModel
}

func getShardingName(connPoolMap map[ShardingName][]gorm.ConnPool) RouteMode {
	for name, _ := range connPoolMap {
		return RouteMode(name)
	}
	return "default"
}

func (r *route) mark(stmt *gorm.Statement, shardingName ShardingName) {
	if r.traceRouteMode {
		if shardingName == "" {
			markStmtResolverMode(stmt, RouteModeSlave)
		} else {
			markStmtResolverMode(stmt, RouteMode(shardingName), "-", RouteModeMaster)
		}
	}
}

// rewriteSql
//
//	@Description: 重写sql，分表实现
//	@param stmt
//	@param sql	带占位符的sql
//	@return string
func (r *route) rewriteSql(stmt *gorm.Statement, sql string) string {
	ctx := stmt.Context
	tbPolicyResult := r.tbPolicy.Resolve(ctx, stmt.Table, sql, stmt.Logger)
	return tbPolicyResult.Sql
}

// route
//
//	@Description: 路由指定实例，分库实现
//	@param stmt
//	@param sql 填充了参数值的sql
//	@param op
//	@return connPool
func (r *route) route(stmt *gorm.Statement, sql string, op Operation) (connPool gorm.ConnPool) {
	if op == Read {
		if r.slaves != nil {
			result := r.dbPolicy.Resolve(stmt.Context, r.slaves, stmt.Table, sql, stmt.Logger)
			connPool = result.ConnPool
			r.mark(stmt, result.Name)
		} else {
			result := r.dbPolicy.Resolve(stmt.Context, r.masters, stmt.Table, sql, stmt.Logger)
			connPool = result.ConnPool
			r.mark(stmt, result.Name)
		}
	} else {
		result := r.dbPolicy.Resolve(stmt.Context, r.masters, stmt.Table, sql, stmt.Logger)
		connPool = result.ConnPool
		r.mark(stmt, result.Name)
	}

	if stmt.DB.PrepareStmt {
		if preparedStmt, ok := r.dbRoute.prepareStmtStore[connPool]; ok {
			return &gorm.PreparedStmtDB{
				ConnPool: connPool,
				Mux:      preparedStmt.Mux,
				Stmts:    preparedStmt.Stmts,
			}
		}
	}
	return
}

func (r *route) call(fc func(connPool gorm.ConnPool) error) error {
	for _, pools := range r.masters {
		for _, pool := range pools {
			if err := fc(pool); err != nil {
				return err
			}
		}
	}
	for _, pools := range r.slaves {
		for _, pool := range pools {
			if err := fc(pool); err != nil {
				return err
			}
		}
	}
	return nil
}
