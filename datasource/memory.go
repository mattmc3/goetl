package datasource

type MemoryReader struct {
	Data [][]interface{}
	BaseReader
}

// NewMemoryReader constructor
func NewMemoryReader(recordType string, data [][]interface{}) *MemoryReader {
	fields := make(map[string][]string)
	result := MemoryReader{
		Data: data,
		BaseReader: BaseReader{
			fields:     fields,
			recordType: recordType,
		},
	}
	return &result
}

// ReadNext reads the next record if one is available. When the end of the data
// set is reached, the EndOfRecords error is returned.
func (g *MemoryReader) ReadNext() ([]interface{}, error) {
	idx := &g.recordNumber
	if *idx >= len(g.Data) {
		return nil, EndOfRecords
	}
	result := g.Data[*idx]
	*idx++

	fieldNames, ok := g.fields[g.recordType]
	if !ok || len(result) != len(fieldNames) {
		g.fields[g.recordType] = GenerateDefaultFieldNames(len(result))
	}
	return result, nil
}

// MemoryWriter writes records to an in-memory data structure
type MemoryWriter struct {
	Data map[string][]interface{}
	BaseWriter
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