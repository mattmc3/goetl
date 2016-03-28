package etl_test

import (
	"reflect"
	"testing"

	"github.com/mattmc3/goetl/datasource"
	"github.com/mattmc3/goetl/etl"
)

type TestParams struct {
	fields []string
	data   [][]interface{}
}

var testCases = []TestParams{
	{
		[]string{},
		[][]interface{}{},
	}, // no data
	{
		[]string{"Field001", "Field002", "Field003"},
		[][]interface{}{
			[]interface{}{"a", "b", "c"},
			[]interface{}{1, 2, 3},
			[]interface{}{2, 4, 6},
			[]interface{}{3, 6, 9},
		},
	},
}

func TestPushData(t *testing.T) {
	for _, test := range testCases {
		rdr := datasource.NewMemoryReader("testdata", test.data)
		wtr := datasource.NewMemoryWriter()
		err := etl.PushData(rdr, wtr)
		if err != nil {
			t.Errorf(`Extract() errored: %v`, err)
		}

		if wtr.RecordsWritten() != len(test.data) {
			t.Errorf(`RecordsWritten() = %v; want %v`, wtr.RecordsWritten(), len(test.data))
		}

		rec, err := rdr.ReadNext()
		if rec != nil {
			t.Errorf(`Extract() did not consume all available reader records. %v`, rec)
		} else if err != datasource.EndOfRecords {
			t.Errorf(`Extract() errored: %v`, err)
		}

		for i := 0; i < len(wtr.Data); i++ {
			expected := test.data[i]
			actual := wtr.Data["testdata"][i]
			if !reflect.DeepEqual(expected, actual) {
				t.Errorf(`Extract()#%v = %v; want %v`, i, actual, expected)
			}
		}
	}
}
