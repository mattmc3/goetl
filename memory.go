package goetl

import "sort"

// MemoryReader is a reader for an in-memory (slice)
type MemoryReader struct {
	Data         [][]interface{}
	recordType   string
	fieldNames   map[string][]string
	recordNumber int
}

// NewMemoryReader constructor
func NewMemoryReader(recordType string, data [][]interface{}) *MemoryReader {
	fieldNames := make(map[string][]string)
	result := MemoryReader{
		Data:         data,
		fieldNames:   fieldNames,
		recordType:   recordType,
		recordNumber: 0,
	}
	return &result
}

// ReadNext reads the next record if one is available. When the end of the data
// set is reached, the ErrEndOfRecords error is returned.
func (g *MemoryReader) ReadNext() ([]interface{}, error) {
	idx := &g.recordNumber
	if *idx >= len(g.Data) {
		return nil, ErrEndOfRecords
	}
	result := g.Data[*idx]
	*idx++

	fieldNames, ok := g.fieldNames[g.recordType]
	if !ok || len(result) != len(fieldNames) {
		g.fieldNames[g.recordType] = GenerateColumnNames(len(result), FieldNumStyle)
	}
	return result, nil
}

// FieldNames returns the field names for the last read record type
func (g *MemoryReader) FieldNames() []string {
	return g.fieldNames[g.recordType]
}

// RecordType returns the name of the last read record type
func (g *MemoryReader) RecordType() string {
	return g.recordType
}

// AllRecordTypes returns the names  of all the read record types
func (g *MemoryReader) AllRecordTypes() []string {
	rectypes := make([]string, 0, len(g.fieldNames))
	for key := range g.fieldNames {
		rectypes = append(rectypes, key)
	}
	sort.Strings(rectypes)
	return rectypes
}

// MemoryWriter writes records to an in-memory data structure
type MemoryWriter struct {
	Data    map[string][]interface{}
	records int
}

// NewMemoryWriter constructor
func NewMemoryWriter() *MemoryWriter {
	data := make(map[string][]interface{})
	result := MemoryWriter{
		Data: data,
	}
	return &result
}

// Write adds the records provided to the in-memory data structure.
func (g *MemoryWriter) Write(recordType string, record []interface{}) error {
	g.Data[recordType] = append(g.Data[recordType], record)
	g.records++
	return nil
}

// RecordsWritten returns the number of records written
func (g *MemoryWriter) RecordsWritten() int {
	return g.records
}
