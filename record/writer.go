package record

// Writer is an interface for writing data records
type Writer interface {
	// Write takes a record type and the record data and writes that record to
	// the appropriate destination
	Write(string, []interface{}) error
	RecordsWritten() int
}
