package dbroute

import (
	"fmt"
	"github.com/Knetic/govaluate"
	"gorm/dbroute/util/str"
	"strconv"
)

// parseExpression 分表表达式解析
func parseExpression(parameter string, expression string, value interface{}) interface{} {
	functions := map[string]govaluate.ExpressionFunction{
		"parse": func(args ...interface{}) (interface{}, error) {
			str := ""
			for _, arg := range args {
				str += fmt.Sprintf("%v", arg)
			}
			return str, nil
		},
		"hashcode": func(args ...interface{}) (interface{}, error) {
			return str.Hashcode(fmt.Sprintf("%v", args[0])), nil
		},
		"mod": func(args ...interface{}) (interface{}, error) {
			a, _ := strconv.ParseInt(fmt.Sprintf("%v", args[0]), 10, 64)
			b, _ := strconv.ParseInt(fmt.Sprintf("%v", args[1]), 10, 64)
			return a % b, nil
		},
	}
	expressionRes, _ := govaluate.NewEvaluableExpressionWithFunctions(expression, functions)
	parameters := make(map[string]interface{})
	parameters[parameter] = value
	result, _ := expressionRes.Evaluate(parameters)
	return result
}
