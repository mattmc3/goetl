package datasource_test

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/mattmc3/goetl/datasource"
)

type CsvTestParams struct {
	data string
	ReaderTestParams
}

var csvTestCases = []CsvTestParams{
	{
		"",
		ReaderTestParams{
			"empty",
			[]string{},
			[][]interface{}{},
		},
	},
	{
		"\"A\",\"B\",\"C\"\n1,2,3\n2,4,6\n3,6,9\n",
		ReaderTestParams{
			"3x3 recs w/ header",
			[]string{"A", "B", "C"},
			[][]interface{}{
				[]interface{}{"A", "B", "C"},
				[]interface{}{"1", "2", "3"},
				[]interface{}{"2", "4", "6"},
				[]interface{}{"3", "6", "9"},
			},
		},
	},
}

func TestCSVReader(t *testing.T) {
	for _, test := range csvTestCases {
		csvrdr := csv.NewReader(strings.NewReader(test.data))
		rdr := datasource.NewCSVReader(test.recordType, csvrdr, true)
		VerifyReader(t, rdr, test.ReaderTestParams)
	}
}

//
// func TestMemoryWriter(t *testing.T) {
// 	for _, test := range testCases {
// 		wtr := datasource.NewMemoryWriter()
// 		for i, rec := range test.data {
// 			if i != wtr.RecordsWritten() {
// 				t.Errorf(`RecordsWritten()#%v = %v; want %v`, i, i, wtr.RecordsWritten())
// 			}
//
// 			datalen := len(wtr.Data[test.recordType])
// 			if datalen != i {
// 				t.Errorf(`len(.Data)#%v = %v; want %v`, i, datalen, i)
// 			}
//
// 			wtr.Write(test.recordType, rec)
//
// 			actual := wtr.Data[test.recordType][i]
// 			if !reflect.DeepEqual(actual, rec) {
// 				t.Errorf(`Write()#%v = %v; want %v`, i, actual, rec)
// 			}
// 		}
// 	}
// }
