package expand

import (
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"reflect"
)

// ClearWhereTableName
//
//	@Description: 清空where条件中的前置表名
//	@param db
func ClearWhereTableName(db *gorm.DB) {
	// 前置将Exprs[]#Cloumn的Table清空，避免出现携带条件带表名
	if cs, ok := db.Statement.Clauses["WHERE"]; ok {
		if whereClause, ok := cs.Expression.(clause.Where); ok {
			for index, expr := range whereClause.Exprs {
				switch e := expr.(type) {
				case clause.Eq:
					if col, ok := e.Column.(clause.Column); ok {
						e.Column = clause.Column{
							Table: "",
							Name:  col.Name,
							Alias: col.Alias,
							Raw:   col.Raw,
						}
						whereClause.Exprs[index] = e
					}
				case clause.Neq:
					if col, ok := e.Column.(clause.Column); ok {
						e.Column = clause.Column{
							Table: "",
							Name:  col.Name,
							Alias: col.Alias,
							Raw:   col.Raw,
						}
						whereClause.Exprs[index] = e
					}
				case clause.Gt:
					if col, ok := e.Column.(clause.Column); ok {
						e.Column = clause.Column{
							Table: "",
							Name:  col.Name,
							Alias: col.Alias,
							Raw:   col.Raw,
						}
						whereClause.Exprs[index] = e
					}
				case clause.Gte:
					if col, ok := e.Column.(clause.Column); ok {
						e.Column = clause.Column{
							Table: "",
							Name:  col.Name,
							Alias: col.Alias,
							Raw:   col.Raw,
						}
						whereClause.Exprs[index] = e
					}
				case clause.Lt:
					if col, ok := e.Column.(clause.Column); ok {
						e.Column = clause.Column{
							Table: "",
							Name:  col.Name,
							Alias: col.Alias,
							Raw:   col.Raw,
						}
						whereClause.Exprs[index] = e
					}
				case clause.Lte:
					if col, ok := e.Column.(clause.Column); ok {
						e.Column = clause.Column{
							Table: "",
							Name:  col.Name,
							Alias: col.Alias,
							Raw:   col.Raw,
						}
						whereClause.Exprs[index] = e
					}
				case clause.Like:
					if col, ok := e.Column.(clause.Column); ok {
						e.Column = clause.Column{
							Table: "",
							Name:  col.Name,
							Alias: col.Alias,
							Raw:   col.Raw,
						}
						whereClause.Exprs[index] = e
					}
				}
			}
		}
	}
}

// PreBuildSql
//
//	@Description: 提前构造SQL，用于路由
//	@param db
func PreBuildSql(db *gorm.DB) {
	if db.Statement.SQL.Len() == 0 {
		// pre-generated SQL
		switch db.Statement.BuildClauses[0] {
		case "INSERT":
			db.Statement.SQL.Grow(180)
			db.Statement.AddClauseIfNotExists(clause.Insert{})
			db.Statement.AddClause(callbacks.ConvertToCreateValues(db.Statement))
			db.Statement.Build(db.Statement.BuildClauses...)
		case "UPDATE":
			db.Statement.SQL.Grow(180)
			db.Statement.AddClauseIfNotExists(clause.Update{})
			if _, ok := db.Statement.Clauses["SET"]; !ok {
				if set := callbacks.ConvertToAssignments(db.Statement); len(set) != 0 {
					db.Statement.AddClause(set)
				} else {
					return
				}
			}
			db.Statement.Build(db.Statement.BuildClauses...)
		case "SELECT":
			callbacks.BuildQuerySQL(db)
		case "DELETE":
			db.Statement.SQL.Grow(100)
			db.Statement.AddClauseIfNotExists(clause.Delete{})
			if db.Statement.Schema != nil {
				_, queryValues := schema.GetIdentityFieldValuesMap(db.Statement.Context, db.Statement.ReflectValue, db.Statement.Schema.PrimaryFields)
				column, values := schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

				if len(values) > 0 {
					db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
				}

				if db.Statement.ReflectValue.CanAddr() && db.Statement.Dest != db.Statement.Model && db.Statement.Model != nil {
					_, queryValues = schema.GetIdentityFieldValuesMap(db.Statement.Context, reflect.ValueOf(db.Statement.Model), db.Statement.Schema.PrimaryFields)
					column, values = schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

					if len(values) > 0 {
						db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
					}
				}
			}
			db.Statement.AddClauseIfNotExists(clause.From{})
			db.Statement.Build(db.Statement.BuildClauses...)
		default:
		}
	}
}
