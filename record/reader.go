package record

import (
	"errors"
	"sort"
)

// EndOfRecords is the non-error error returned by a Reader when no more records
// are available to read. Functions should only return EndOfRecords to signal a
// graceful end to reading records. If EndOfRecords occurs unexpectedly, a
// detailed 'real' error is more appropriate. This must be implemented as an
// error rather than a simple bool property on a Reader because the answer is
// not known prior to calling ReadNext().
var EndOfRecords = errors.New("end of records reached")

// Reader is an interface for reading data records
type Reader interface {
	ReadNext() ([]interface{}, error)
	Fields() []string
	RecordType() string
}

type BaseReader struct {
	recordType   string
	fields       map[string][]string
	recordNumber int
}

// Fields returns the field names for the last read record type
func (g *BaseReader) Fields() []string {
	return g.fields[g.recordType]
}

// RecordType returns the name of the last read record type
func (g *BaseReader) RecordType() string {
	return g.recordType
}

// AllRecordTypes returns the names  of all the read record types
func (g *BaseReader) AllRecordTypes() []string {
	rectypes := make([]string, 0, len(g.fields))
	for key := range g.fields {
		rectypes = append(rectypes, key)
	}
	sort.Strings(rectypes)
	return rectypes
}
