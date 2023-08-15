package str

import (
	"encoding/json"
	"fmt"
	"math"
)

// Hashcode 计算字符串的hashcode
func Hashcode(s string) int32 {
	var hash int32 = 0
	for _, c := range s {
		hash = c + ((hash << 5) - hash)
	}
	return hash
}

// HashMode 计算字符串的hashcode后取余
func HashMode(s string, num int32) int {
	hash := Hashcode(s)
	return int(math.Abs(float64(hash % num)))
}

// ConvertStrToStruct 字符串转对象
func ConvertStrToStruct(str string, v any) error {
	err := json.Unmarshal([]byte(str), &v)
	if err != nil {
		fmt.Errorf("unmarshal err: %v", err)
		return err
	}
	return nil
}
