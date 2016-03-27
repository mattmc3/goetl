package record_test

import (
	"reflect"
	"testing"

	"github.com/mattmc3/goetl/record"
)

type DefaultFieldNameTestParams struct {
	fields   int
	expected []string
}

func TestGenerateDefaultFieldNames(t *testing.T) {
	testCases := []DefaultFieldNameTestParams{
		{1, []string{"Field001"}},
		{3, []string{"Field001", "Field002", "Field003"}},
	}

	for _, test := range testCases {
		actual := record.GenerateDefaultFieldNames(test.fields)
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf(`GenerateDefaultFieldNames(%v) = %v; want %v`, test.fields, actual, test.expected)
		}
	}
}
