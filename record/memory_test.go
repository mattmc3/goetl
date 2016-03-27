package record_test

import (
	"reflect"
	"testing"

	"github.com/mattmc3/goetl/record"
)

type MemoryTestParams struct {
	recordType string
	fields     []string
	data       [][]interface{}
}

var testCases = []MemoryTestParams{
	{
		"empty",
		[]string{},
		[][]interface{}{},
	}, // no data
	{
		"3x3 recs w/ header",
		[]string{"Field001", "Field002", "Field003"},
		[][]interface{}{
			[]interface{}{"a", "b", "c"},
			[]interface{}{1, 2, 3},
			[]interface{}{2, 4, 6},
			[]interface{}{3, 6, 9},
		},
	},
}

func TestMemoryReader(t *testing.T) {
	for _, test := range testCases {
		rdr := record.NewMemoryReader(test.recordType, test.data)
		for i := 0; i <= len(test.data); i++ {
			actual, err := rdr.ReadNext()

			actualRecTyp := rdr.RecordType()
			if rdr.RecordType() != test.recordType {
				t.Errorf(`RecordType()#%v = %v; want %v`, i, actualRecTyp, test.recordType)
			}

			if i >= len(test.data) {
				if actual != nil || err != record.EndOfRecords {
					t.Errorf(`ReadNext()#%v = (%v, %v); want (%v, %v)`, i, actual, err, nil, record.EndOfRecords)
				}
			} else {
				if !reflect.DeepEqual(actual, test.data[i]) {
					t.Errorf(`ReadNext()#%v = %v; want %v`, i, actual, test.data[i])
				}
				actualFields := rdr.Fields()
				if !reflect.DeepEqual(actualFields, test.fields) {
					t.Errorf(`Fields()#%v = %v; want %v`, i, actualFields, test.fields)
				}
			}
		}
	}
}

func TestMemoryWriter(t *testing.T) {
	for _, test := range testCases {
		wtr := record.NewMemoryWriter()
		for i, rec := range test.data {
			if i != wtr.RecordsWritten() {
				t.Errorf(`RecordsWritten()#%v = %v; want %v`, i, i, wtr.RecordsWritten())
			}

			datalen := len(wtr.Data[test.recordType])
			if datalen != i {
				t.Errorf(`len(.Data)#%v = %v; want %v`, i, datalen, i)
			}

			wtr.Write(test.recordType, rec)

			actual := wtr.Data[test.recordType][i]
			if !reflect.DeepEqual(actual, rec) {
				t.Errorf(`Write()#%v = %v; want %v`, i, actual, rec)
			}
		}
	}
}
