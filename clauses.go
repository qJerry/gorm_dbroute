package dbroute

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Operation string

type ShardingName string

const (
	writeName = "gorm:db_route:write"
	readName  = "gorm:db_route:read"
	usingName = "gorm:db_route:using"
)

// Use specifies configuration
func Use(str string) clause.Expression {
	return using{Use: str}
}

type using struct {
	Use string
}

// ModifyStatement modify operation mode
func (u using) ModifyStatement(stmt *gorm.Statement) {
	stmt.Clauses[usingName] = clause.Clause{Expression: u}
	if fc := stmt.DB.Callback().Query().Get("gorm:db_route"); fc != nil {
		fc(stmt.DB)
	}
}

// Build implements clause.Expression interface
func (u using) Build(clause.Builder) {
}
