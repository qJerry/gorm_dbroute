package dbroute

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"regexp"
	"strconv"
)

// GetSqlTableNameAndCommandType 从sql中取表名
func GetSqlTableNameAndCommandType(sql string) (string, CommandType) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		panic(fmt.Errorf("parse sql err: %w", err))
	}
	switch node := stmt.(type) {
	case *sqlparser.Select:
		if expr, ok := node.From[0].(*sqlparser.AliasedTableExpr); ok {
			return expr.Expr.(sqlparser.TableName).Name.String(), SELECT
		}
	case *sqlparser.Insert:
		return node.Table.Name.String(), INSERT
	case *sqlparser.Update:
	case *sqlparser.Delete:
		if expr, ok := node.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
			return expr.Expr.(sqlparser.TableName).Name.String(), UPDATE
		}
	}
	panic(fmt.Errorf("table_name not found: %w", err))
}

// GetSqlCommandType 从sql中取表名
func GetSqlCommandType(sql string) CommandType {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		panic(fmt.Errorf("parse sql err: %w", err))
	}
	switch stmt.(type) {
	case *sqlparser.Select:
		return SELECT
	case *sqlparser.Insert:
		return INSERT
	case *sqlparser.Update:
	case *sqlparser.Delete:
		return UPDATE
	}
	panic(fmt.Errorf("commondType not found: %w", err))
}

// GetSqlParameterValue 从sql中按key取值
func GetSqlParameterValue(sql string, key string) interface{} {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		panic(fmt.Errorf("parse sql err: %w", err))
	}
	return getSqlParameterValue(stmt, key)
}

// ChangeSqlTableName 更新sql中的表名
func ChangeSqlTableName(sql string, newTableName string) string {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		panic(fmt.Errorf("parse sql err: %w", err))
	}
	newSql := sql
	switch node := stmt.(type) {
	case *sqlparser.Select:
		if expr, ok := node.From[0].(*sqlparser.AliasedTableExpr); ok {
			t := expr.Expr.(sqlparser.TableName)
			expr := sqlparser.AliasedTableExpr{
				Expr: sqlparser.TableName{
					// 表名
					Name: sqlparser.NewTableIdent(newTableName),
					// 库名
					Qualifier: sqlparser.NewTableIdent(t.Qualifier.String()),
				},
				Partitions: nil,
				As:         sqlparser.NewTableIdent(""),
				Hints:      nil,
			}
			node.From = sqlparser.TableExprs{&expr}
			newSql = sqlparser.String(node)
		} else {
			panic(fmt.Errorf("not support parser: %v", node.From[0]))
		}
	case *sqlparser.Insert:
		name := sqlparser.NewTableIdent(newTableName)
		node.Table.Name = name
		newSql = sqlparser.String(node)
	case *sqlparser.Update:
	case *sqlparser.Delete:
		if expr, ok := node.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
			t := expr.Expr.(sqlparser.TableName)
			expr := sqlparser.AliasedTableExpr{
				Expr: sqlparser.TableName{
					// 表名
					Name: sqlparser.NewTableIdent(newTableName),
					// 库名
					Qualifier: sqlparser.NewTableIdent(t.Qualifier.String()),
				},
				Partitions: nil,
				As:         sqlparser.NewTableIdent(""),
				Hints:      nil,
			}
			node.TableExprs = sqlparser.TableExprs{&expr}
			newSql = sqlparser.String(node)
		} else {
			panic(fmt.Errorf("not support parser: %v", node.TableExprs[0]))
		}
	}
	// sqlparser生成的sql中，原sql带有?会被替换成:v+数字，需对其做替换
	pattern := `:v\d+`
	reg := regexp.MustCompile(pattern)
	newSql = reg.ReplaceAllString(newSql, "?")
	return newSql
}

// 通过解析器遍历按key取值
func getSqlParameterValue(parser any, key string) interface{} {
	switch node := parser.(type) {
	case *sqlparser.Select:
		if node.Where != nil {
			return getSqlParameterValue(node.Where.Expr, key)
		}
	case *sqlparser.Insert:
	case *sqlparser.ComparisonExpr:
		if name, ok := node.Left.(*sqlparser.ColName); ok {
			if name.Name.CompliantName() == key {
				println("name: ", name.Name.CompliantName())
				right := node.Right.(*sqlparser.SQLVal)
				switch right.Type {
				case sqlparser.StrVal:
					return string(right.Val)
				case sqlparser.IntVal:
					strVal := string(right.Val)
					value, err := strconv.ParseInt(strVal, 10, 64)
					if err != nil {
						panic(fmt.Errorf("parse int err: %w", err))
					}
					return value
				case sqlparser.FloatVal:
					strVal := string(right.Val)
					value, err := strconv.ParseFloat(strVal, 64)
					if err != nil {
						panic(fmt.Errorf("parse float err: %w", err))
					}
					return value
				}
			}
		}
	case *sqlparser.OrExpr:
	case *sqlparser.AndExpr:
		result := getSqlParameterValue(node.Left, key)
		if result != nil {
			return result
		}
		return getSqlParameterValue(node.Right, key)
	case *sqlparser.BinaryExpr:
	case *sqlparser.IsExpr:
	}
	return nil
}
