package dbroute

import (
	"context"
	"fmt"
	"gorm.io/gorm/logger"
)

type ShardingIndexKey string

const ShardingTableIndex ShardingIndexKey = "tableIndex_%s"

// TbPolicy Table Routing Policy
type TbPolicy interface {
	Resolve(context.Context, string, string, logger.Interface) TbPolicyResult
}

type TbPolicyResult struct {
	ActualTableName string
	Sql             string
}

// TbDefaultPolicy 默认路由，空实现
type TbDefaultPolicy struct {
}

func (TbDefaultPolicy) Resolve(_ context.Context, _ string, sql string, _ logger.Interface) (result TbPolicyResult) {
	return TbPolicyResult{Sql: sql}
}

// TbShardingRoutePolicy 分表路由
type TbShardingRoutePolicy struct {
	// 需要操作分库分表
	DataShardingRuleModelMap map[string]DataShardingRuleModel
}

func (p *TbShardingRoutePolicy) Resolve(ctx context.Context, tableName string, sql string, log logger.Interface) (result TbPolicyResult) {
	if _, ok := p.DataShardingRuleModelMap[tableName]; !ok {
		return TbPolicyResult{Sql: sql}
	} else {
		// update sql
		tableIndexVal := ctx.Value(fmt.Sprintf(string(ShardingTableIndex), tableName))
		if tableIndexVal != nil {
			index := tableIndexVal.(int)
			// 解析得到真正的表名
			actualTableName := fmt.Sprintf("%v_%v", tableName, index)
			log.Info(ctx, "table pre_set sharding: %v", actualTableName)
			return TbPolicyResult{ActualTableName: actualTableName, Sql: ChangeSqlTableName(sql, actualTableName)}
		} else {
			model := p.DataShardingRuleModelMap[tableName]
			// 分库键值
			value := GetSqlParameterValue(sql, model.DatabaseShardingParameter)
			result := parseExpression(model.TableShardingParameter, model.TableShardingExpression, value)
			// 解析得到真正的表名
			actualTableName := result.(string)
			log.Info(ctx, "table sharding: %v", actualTableName)
			// update sql
			return TbPolicyResult{ActualTableName: actualTableName, Sql: ChangeSqlTableName(sql, actualTableName)}
		}
	}
}
