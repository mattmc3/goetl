package datasource

// Writer is an interface for writing data records
type Writer interface {
	// Write takes a record type and the record data and writes that record to
	// the appropriate destination
	Write(string, []interface{}) error
	RecordsWritten() int
}

// BaseWriter handles some common functionality for a typical Writer
// implementation.
type BaseWriter struct {
	records int
}

// RecordsWritten returns the total number of records added by the writer.
func (g *BaseWriter) RecordsWritten() int {
	return g.records
}
