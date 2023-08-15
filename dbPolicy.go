package dbroute

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/rand"
)

type ShardingDbKey string

const ShardingDbIndex ShardingDbKey = "dbIndex_%s"

// DbPolicy Data Source Routing Policy
type DbPolicy interface {
	Resolve(context.Context, map[ShardingName][]gorm.ConnPool, string, string, logger.Interface) DbPolicyResult
}

type DbPolicyResult struct {
	Name     ShardingName
	ConnPool gorm.ConnPool
}

// DbRandomPolicy 随机路由
type DbRandomPolicy struct {
}

func (DbRandomPolicy) Resolve(_ context.Context, connPoolsMap map[ShardingName][]gorm.ConnPool, _ string, _ string, _ logger.Interface) (result DbPolicyResult) {
	result = DbPolicyResult{}
	for name, connPools := range connPoolsMap {
		result = DbPolicyResult{Name: name, ConnPool: connPools[rand.Intn(len(connPools))]}
		break
	}

	return result
}

// DbShardingRoutePolicy 分库路由
type DbShardingRoutePolicy struct {
	// 需要操作分库分表
	DataShardingRuleModelMap map[string]DataShardingRuleModel
}

func (p *DbShardingRoutePolicy) Resolve(ctx context.Context, connPoolsMap map[ShardingName][]gorm.ConnPool, tableName string, sql string, log logger.Interface) (result DbPolicyResult) {
	result = DbPolicyResult{}
	if _, ok := p.DataShardingRuleModelMap[tableName]; !ok {
		// 不存在，走随机路由
		for name, connPools := range connPoolsMap {
			result = DbPolicyResult{Name: name, ConnPool: connPools[rand.Intn(len(connPools))]}
			return result
		}
	}
	dbIndexVal := ctx.Value(fmt.Sprintf(string(ShardingDbIndex), tableName))
	if dbIndexVal != nil {
		// 预设好了索引，直接获取并返回
		shardingKey := ShardingName(dbIndexVal.(string))
		log.Info(ctx, "database pre_set sharding: %v", shardingKey)
		connPools := connPoolsMap[shardingKey]
		return DbPolicyResult{Name: shardingKey, ConnPool: connPools[rand.Intn(len(connPools))]}
	}
	model := p.DataShardingRuleModelMap[tableName]
	if model.DatabaseShardingParameter == "" && model.DatabaseDefaultShardingValue == "" {
		// 不存在，走随机路由
		for name, connPools := range connPoolsMap {
			result = DbPolicyResult{Name: name, ConnPool: connPools[rand.Intn(len(connPools))]}
			return result
		}
	}
	var connPools []gorm.ConnPool
	var shardingKey ShardingName
	if model.DatabaseDefaultShardingValue != "" {
		shardingKey = ShardingName(model.DatabaseDefaultShardingValue)
		connPools = connPoolsMap[ShardingName(model.DatabaseDefaultShardingValue)]
	} else {
		// 分库键值
		value := GetSqlParameterValue(sql, model.DatabaseShardingParameter)
		expressionResult := parseExpression(model.DatabaseShardingParameter, model.DatabaseShardingExpression, value)
		shardingKey = expressionResult.(ShardingName)
		log.Info(ctx, "database sharding: %v", shardingKey)
		// 归属的连接池
		connPools = connPoolsMap[shardingKey]
	}

	// 随机选取一个连接池
	return DbPolicyResult{Name: shardingKey, ConnPool: connPools[rand.Intn(len(connPools))]}
}
