package fixedwidth_test

// import (
// 	"encoding/csv"
// 	"reflect"
// 	"strings"
// 	"testing"

// 	"github.com/mattmc3/goetl"
// 	"github.com/mattmc3/goetl/fixedwidth"
// )

// var fwtests = []struct {
// 	in      string
// 	padWith rune
// 	length  int
// 	left    string
// 	right   string
// 	center  string
// }{
// 	{
// 		"ABC\n123\n456",
// 		[]string{"A", "B", "C"},
// 		[][]interface{}{
// 			{"A", "B", "C"},
// 			{"1", "2", "3"},
// 			{"2", "4", "6"},
// 			{"3", "6", "9"},
// 		},
// 	}
// }

// func TestReader(t *testing.T) {
// 	for _, test := range csvTestCases {
// 		csvrdr := csv.NewReader(strings.NewReader(test.data))
// 		rdr := delimited.NewReader(test.recordType, csvrdr, true)
// 		VerifyReader(t, rdr, test)
// 	}
// }

// func VerifyReader(t *testing.T, rdr goetl.Reader, params CsvTestParams) {
// 	for i := 0; i <= len(params.output); i++ {
// 		actual, err := rdr.ReadNext()

// 		actualRecTyp := rdr.RecordType()
// 		if rdr.RecordType() != params.recordType {
// 			t.Errorf(`RecordType()#%v = %v; want %v`, i, actualRecTyp, params.recordType)
// 		}

// 		if i >= len(params.output) {
// 			if actual != nil || err != goetl.ErrEndOfRecords {
// 				t.Errorf(`ReadNext()#%v = (%v, %v); want (%v, %v)`, i, actual, err, nil, goetl.ErrEndOfRecords)
// 			}
// 		} else {
// 			if !reflect.DeepEqual(actual, params.output[i]) {
// 				t.Errorf(`ReadNext()#%v = %v; want %v`, i, actual, params.output[i])
// 			}
// 			actualFields := rdr.FieldNames()
// 			if !reflect.DeepEqual(actualFields, params.fieldNames) {
// 				t.Errorf(`FieldNames()#%v = %v; want %v`, i, actualFields, params.fieldNames)
// 			}
// 		}
// 	}
// }
