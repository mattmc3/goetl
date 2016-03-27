package record

import "fmt"

func GenerateDefaultFieldNames(numFields int) []string {
	if numFields <= 0 {
		return []string{}
	}
	result := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		result[i] = fmt.Sprintf("Field%03d", i+1)
	}
	return result
}
