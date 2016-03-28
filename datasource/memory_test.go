package datasource_test

import (
	"reflect"
	"testing"

	"github.com/mattmc3/goetl/datasource"
)

type ReaderTestParams struct {
	recordType string
	fields     []string
	output     [][]interface{}
}

type MemoryTestParams struct {
	ReaderTestParams
}

var memoryTestCases = []MemoryTestParams{
	{
		ReaderTestParams{
			"empty",
			[]string{},
			[][]interface{}{},
		},
	},
	{
		ReaderTestParams{
			"3x3 recs w/ header",
			[]string{"Field001", "Field002", "Field003"},
			[][]interface{}{
				[]interface{}{"a", "b", "c"},
				[]interface{}{1, 2, 3},
				[]interface{}{2, 4, 6},
				[]interface{}{3, 6, 9},
			},
		},
	},
}

func TestMemoryReader(t *testing.T) {
	for _, test := range memoryTestCases {
		rdr := datasource.NewMemoryReader(test.recordType, test.output)
		VerifyReader(t, rdr, test.ReaderTestParams)
	}
}

func VerifyReader(t *testing.T, rdr datasource.Reader, params ReaderTestParams) {
	for i := 0; i <= len(params.output); i++ {
		actual, err := rdr.ReadNext()

		actualRecTyp := rdr.RecordType()
		if rdr.RecordType() != params.recordType {
			t.Errorf(`RecordType()#%v = %v; want %v`, i, actualRecTyp, params.recordType)
		}

		if i >= len(params.output) {
			if actual != nil || err != datasource.EndOfRecords {
				t.Errorf(`ReadNext()#%v = (%v, %v); want (%v, %v)`, i, actual, err, nil, datasource.EndOfRecords)
			}
		} else {
			if !reflect.DeepEqual(actual, params.output[i]) {
				t.Errorf(`ReadNext()#%v = %v; want %v`, i, actual, params.output[i])
			}
			actualFields := rdr.Fields()
			if !reflect.DeepEqual(actualFields, params.fields) {
				t.Errorf(`Fields()#%v = %v; want %v`, i, actualFields, params.fields)
			}
		}
	}
}

func TestMemoryWriter(t *testing.T) {
	for _, test := range memoryTestCases {
		wtr := datasource.NewMemoryWriter()
		for i, rec := range test.output {
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
