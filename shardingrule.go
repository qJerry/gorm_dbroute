package dbroute

// DataShardingRuleModel 数据分片规则
type DataShardingRuleModel struct {
	Table                        string `json:"table"`
	DatabaseDefaultShardingValue string `json:"database-default-sharding-value"`
	DatabaseShardingParameter    string `json:"database-sharding-parameter"`
	DatabaseShardingExpression   string `json:"database-sharding-expression"`
	TableShardingParameter       string `json:"table-sharding-parameter"`
	TableShardingExpression      string `json:"table-sharding-expression"`
	Rules                        []Rule `json:"rules"`
}

type Rule struct {
	CommandType             string      `json:"command-type"`
	TableShardingParameter  string      `json:"table-sharding-parameter"`
	TableShardingExpression string      `json:"table-sharding-expression"`
	WriteBack               bool        `json:"write-back"`
	ChildRule               []ChildRule `json:"child-rule"`
}

type ChildRule struct {
	ArchiveParameter string     `json:"archive-parameter"`
	ArchiveMethod    string     `json:"archive-method"`
	Hit              ArchiveHit `json:"hit"`
	Miss             ArchiveHit `json:"miss"`
}

type ArchiveHit struct {
	DatabaseShardingValueIndex int `json:"database-sharding-value-index"`
}

type CommandType string

const (
	SELECT CommandType = "SELECT"
	INSERT CommandType = "INSERT"
	UPDATE CommandType = "UPDATE"
	DELETE CommandType = "DELETE"
)
