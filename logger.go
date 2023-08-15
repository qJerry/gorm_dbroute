package dbroute

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"time"
)

type RouteModeKey string
type RouteMode string

const routeModeKey RouteModeKey = "dbroute:route_mode_key"
const (
	RouteModeMaster RouteMode = "master"
	RouteModeSlave  RouteMode = "slave"
)

type routeModeLogger struct {
	logger.Interface
}

func (l routeModeLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	var splitFn = func() (sql string, rowsAffected int64) {
		sql, rowsAffected = fc()
		op := ctx.Value(routeModeKey)
		if op != nil {
			sql = fmt.Sprintf("[%s] %s", op, sql)
			return
		}

		// the situation that dbresolver does not handle
		// such as transactions, or some resolvers do not enable MarkResolverMode.
		return
	}
	l.Interface.Trace(ctx, begin, splitFn, err)
}

func NewResolverModeLogger(l logger.Interface) logger.Interface {
	if _, ok := l.(routeModeLogger); ok {
		return l
	}
	return routeModeLogger{
		Interface: l,
	}
}

func markStmtResolverMode(stmt *gorm.Statement, mode ...RouteMode) {
	if _, ok := stmt.Logger.(routeModeLogger); ok {
		stmt.Context = context.WithValue(stmt.Context, routeModeKey, mode)
	}
}
