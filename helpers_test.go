package goetl_test

import (
	"reflect"
	"testing"

	"github.com/mattmc3/goetl"
)

var genColumnNameTestCases = []struct {
	columns  int
	expected []string
}{
	{1, []string{"Col001"}},
	{3, []string{"Col001", "Col002", "Col003"}},
}

func TestGenerateColumnNames(t *testing.T) {
	for _, test := range genColumnNameTestCases {
		actual := goetl.GenerateColumnNames(test.columns, goetl.ColNumStyle)
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf(`GenerateColumnNames(%v) = %v; want %v`, test.columns, actual, test.expected)
		}
	}
}

var excelColNameTests = []struct {
	colNum  int
	colName string
}{
	{-1, ""},
	{1, "A"},
	{2, "B"},
	{3, "C"},
	{26, "Z"},
	{27, "AA"},
	{52, "AZ"},
	{53, "BA"},
	{702, "ZZ"},
	{703, "AAA"},
	{1024, "AMJ"},  // LibreOffice Calc
	{256, "IV"},    // Excel <= 2003
	{16384, "XFD"}, // Excel > 2003
}

func TestCalcSpreadsheetColumnName(t *testing.T) {
	for _, test := range excelColNameTests {
		actual := goetl.GetSpreadsheetColumnName(test.colNum)
		if actual != test.colName {
			t.Errorf(`CalcSpreadsheetColumnName(%v) = %v; want %v`, test.colNum, actual, test.colName)
		}
	}
}

func TestCalcSpreadsheetColumnNumber(t *testing.T) {
	for _, test := range excelColNameTests {
		actual := goetl.GetSpreadsheetColumnNumber(test.colName)
		if actual != test.colNum {
			t.Errorf(`GetSpreadsheetColumnNumber(%v) = %v; want %v`, test.colName, actual, test.colName)
		}
	}
}

var genExcelColNameTestCases = []struct {
	columns  int
	expected []string
}{
	{1, []string{"A"}},
	{3, []string{"A", "B", "C"}},
}

func TestGenerateSpreadsheetColumnNames(t *testing.T) {
	for _, test := range genExcelColNameTestCases {
		actual := goetl.GenerateColumnNames(test.columns, goetl.SpreadsheetStyle)
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf(`GenerateSpreadsheetColumnNames(%v) = %v; want %v`, test.columns, actual, test.expected)
		}
	}
}
