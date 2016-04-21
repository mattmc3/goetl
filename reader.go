package goetl

import "errors"

// ErrEndOfRecords is the non-error error returned by a Reader when no more records
// are available to read. Functions should only return EndOfRecords to signal a
// graceful end to reading records. If EndOfRecords occurs unexpectedly, a
// detailed 'real' error is more appropriate. This must be implemented as an
// error rather than a simple bool property on a Reader because the answer is
// not known prior to calling ReadNext().
var ErrEndOfRecords = errors.New("end of records reached")

// Reader is an interface for reading data records
type Reader interface {
	ReadNext() ([]interface{}, error)
	FieldNames() []string
	RecordType() string
	AllRecordTypes() []string
}

// // BaseReader handles some common functionality for a typical Reader
// // implementation.
// type BaseReader struct {
// 	recordType   string
// 	fieldNames   map[string][]string
// 	recordNumber int
// }

// // FieldNames returns the field names for the last read record type
// func (g *BaseReader) FieldNames() []string {
// 	return g.fieldNames[g.recordType]
// }

// // RecordType returns the name of the last read record type
// func (g *BaseReader) RecordType() string {
// 	return g.recordType
// }

// // AllRecordTypes returns the names  of all the read record types
// func (g *BaseReader) AllRecordTypes() []string {
// 	rectypes := make([]string, 0, len(g.fieldNames))
// 	for key := range g.fieldNames {
// 		rectypes = append(rectypes, key)
// 	}
// 	sort.Strings(rectypes)
// 	return rectypes
// }
