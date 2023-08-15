package dbroute

import (
	"gorm.io/gorm"
	"gorm/dbroute/expand"
	"strings"
)

func (dr *DBRoute) registerCallbacks(db *gorm.DB) {
	dr.Callback().Create().Before("*").Register("gorm:db_route", dr.switchMaster)
	dr.Callback().Query().Before("*").Register("gorm:db_route", dr.switchSlave)
	dr.Callback().Update().Before("*").Register("gorm:db_route", dr.switchMaster)
	dr.Callback().Delete().Before("*").Register("gorm:db_route", dr.switchMaster)
	dr.Callback().Row().Before("*").Register("gorm:db_route", dr.switchSlave)
	dr.Callback().Raw().Before("*").Register("gorm:db_route", dr.switchGuess)
}

func (dr *DBRoute) base(db *gorm.DB, op Operation) {
	expand.ClearWhereTableName(db)
	expand.PreBuildSql(db)
	var newSql strings.Builder
	newSql.WriteString(dr.routeTb(db.Statement))
	db.Statement.SQL = newSql
	db.Statement.ConnPool = dr.routeDb(db.Statement, db.Dialector.Explain(db.Statement.SQL.String(), db.Statement.Vars...), op)
}

func (dr *DBRoute) switchMaster(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		dr.base(db, Write)
	}
}

func (dr *DBRoute) switchSlave(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		if rawSQL := db.Statement.SQL.String(); len(rawSQL) > 0 {
			dr.switchGuess(db)
		} else {
			_, locking := db.Statement.Clauses["FOR"]
			if _, ok := db.Statement.Settings.Load(writeName); ok || locking {
				dr.base(db, Write)
			} else {
				dr.base(db, Read)
			}
		}
	}
}

func (dr *DBRoute) switchGuess(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		if _, ok := db.Statement.Settings.Load(writeName); ok {
			dr.base(db, Write)
		} else if rawSQL := strings.TrimSpace(db.Statement.SQL.String()); len(rawSQL) > 10 && strings.EqualFold(rawSQL[:6], "select") && !strings.EqualFold(rawSQL[len(rawSQL)-10:], "for update") {
			dr.base(db, Read)
		} else {
			dr.base(db, Write)
		}
	}
}

func isTransaction(connPool gorm.ConnPool) bool {
	_, ok := connPool.(gorm.TxCommitter)
	return ok
}
