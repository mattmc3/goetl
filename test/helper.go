package test

import (
	"reflect"
	"testing"

	"github.com/mattmc3/goetl"
)

type ReaderWriterTestParams struct {
	RawData     string
	FieldNames  [][]string
	RecordTypes []string
	ParsedData  [][]interface{}
}

// ReadNext() ([]interface{}, error)
// FieldNames() []string
// RecordType() string
// AllRecordTypes() []string

// VerifyReaderInterface is a helper function for those that implement the Reader
// interface to not have to re-invent the wheel testing it.
func VerifyReaderInterface(t *testing.T, rdr goetl.Reader, params ReaderWriterTestParams) {
	for i := 0; i <= len(params.ParsedData); i++ {
		// allow shortcut of 1 record type specified
		var expectedRecTyp string
		if len(params.RecordTypes) == 1 {
			expectedRecTyp = params.RecordTypes[0]
		} else {
			expectedRecTyp = params.RecordTypes[i]
		}

		// allow shortcut of 1 set of field names
		var expectedFields []string
		if len(params.FieldNames) == 1 {
			expectedFields = params.FieldNames[0]
		} else {
			expectedFields = params.FieldNames[i]
		}

		// actual
		actualParsedData, err := rdr.ReadNext()
		actualRecTyp := rdr.RecordType()
		actualFieldNames := rdr.FieldNames()

		if actualRecTyp != expectedRecTyp {
			t.Errorf(`RecordType()#%v = %v; want %v`, i, actualRecTyp, expectedRecTyp)
		}

		if i >= len(params.ParsedData) {
			if actualParsedData != nil || err != goetl.ErrEndOfRecords {
				t.Errorf(`ReadNext()#%v = (%v, %v); want (%v, %v)`, i, actualParsedData, err, nil, goetl.ErrEndOfRecords)
			}
		} else {
			if !reflect.DeepEqual(actualParsedData, params.ParsedData[i]) {
				t.Errorf(`ReadNext()#%v = %v; want %v`, i, actualParsedData, params.ParsedData[i])
			}
			if !reflect.DeepEqual(actualFieldNames, expectedFields) {
				t.Errorf(`FieldNames()#%v = %v; want %v`, i, actualFieldNames, expectedFields)
			}
		}
	}
}
