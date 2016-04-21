package goetl

import (
	"fmt"
	"math"
	"strings"
)

// ColumnNameStyle defines a function signature for naming columns.
type ColumnNameStyle func(colNum int) string

// ColNumStyle returns column names in the format "Col001" ... "Col999"
func ColNumStyle(colNum int) string {
	return fmt.Sprintf("Col%03d", colNum)
}

// FieldNumStyle returns column names in the format "Field001" ... "Field999"
func FieldNumStyle(colNum int) string {
	return fmt.Sprintf("Field%03d", colNum)
}

// SpreadsheetStyle returns column names in the format "A", "B", "C" ... "ZZ"
func SpreadsheetStyle(colNum int) string {
	return GetSpreadsheetColumnName(colNum)
}

// GenerateColumnNames makes a slice of column names for the specified number of
// columns matching the provided style. Ex: 2 -> []string{"Col01", "Col02"}
func GenerateColumnNames(numColumns int, fnStyle ColumnNameStyle) []string {
	if numColumns <= 0 {
		return []string{}
	}
	result := make([]string, numColumns)
	for i := 0; i < numColumns; i++ {
		result[i] = fnStyle(i + 1)
	}
	return result
}

// GetSpreadsheetColumnName takes a one-based column number and calculates the
// spreadsheet-style naming for it. For example, columns 1-3 would be labeled
// [A B C], and then after column 26 (Z), column 27 becomes AA.
func GetSpreadsheetColumnName(colNumber int) string {
	if colNumber <= 0 {
		return ""
	}
	s := ""
	for {
		colNumber--
		s = string('A'+(colNumber%26)) + s
		colNumber /= 26
		if colNumber <= 0 {
			break
		}
	}
	return s
}

// GetSpreadsheetColumnNumber takes a spreadsheet column name and returns the
// corresponding column number. If the column name is not a legit spreadsheet
// name, 0 is returned.
func GetSpreadsheetColumnNumber(columnName string) int {
	if len(columnName) == 0 {
		return -1
	}

	columnName = strings.ToUpper(columnName)
	result := 0
	for i := len(columnName) - 1; i >= 0; i-- {
		ch := rune(columnName[i])
		if ch < 'A' || ch > 'Z' {
			return -1
		}
		val := int(ch) - 64
		result = result + val*int(math.Pow(26.0, float64(len(columnName)-(i+1))))
	}
	return result
}
