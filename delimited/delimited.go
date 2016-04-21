package delimited

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/mattmc3/goetl"
	"github.com/mattmc3/gofurther/slicex"
)

// FileReader wraps the more general purpose Reader to provide file-based
// delimited data.
type FileReader struct {
	reader *Reader
	file   *os.File
}

// NewFileReader constructor
func NewFileReader(recordType string, filePath string, hasHeader bool) (*FileReader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	csvrdr := csv.NewReader(bufio.NewReader(f))
	r := NewReader(recordType, csvrdr, hasHeader)

	result := FileReader{r, f}
	return &result, nil
}

// Close is the clean up method for a FileReader
func (g *FileReader) Close() error {
	return g.file.Close()
}

// Reader handles reading from a delimited source.
type Reader struct {
	reader       *csv.Reader
	hasHeader    bool
	recordType   string
	fieldNames   map[string][]string
	recordNumber int
}

// NewReader constructor
func NewReader(recordType string, rdr *csv.Reader, hasHeader bool) *Reader {
	fieldNames := make(map[string][]string)
	result := Reader{
		reader:       rdr,
		hasHeader:    hasHeader,
		fieldNames:   fieldNames,
		recordType:   recordType,
		recordNumber: 0,
	}
	return &result
}

// FieldNames returns the field names for the last read record type
func (g *Reader) FieldNames() []string {
	return g.fieldNames[g.recordType]
}

// RecordType returns the name of the last read record type
func (g *Reader) RecordType() string {
	return g.recordType
}

// AllRecordTypes returns the names  of all the read record types
func (g *Reader) AllRecordTypes() []string {
	rectypes := make([]string, 0, len(g.fieldNames))
	for key := range g.fieldNames {
		rectypes = append(rectypes, key)
	}
	sort.Strings(rectypes)
	return rectypes
}

// ReadNext reads the next record if one is available. When the end of the data
// set is reached, the EndOfRecords error is returned.
func (g *Reader) ReadNext() ([]interface{}, error) {
	idx := &g.recordNumber
	*idx++

	var rec []string
	var result []interface{}
	var err error

	rec, err = g.reader.Read()
	result = slicex.ObjectifyStrings(rec)
	if err != nil {
		if err == io.EOF {
			return nil, goetl.ErrEndOfRecords
		}
		return result, err
	}

	if *idx == 1 {
		if g.hasHeader {
			g.fieldNames[g.recordType] = rec
		} else {
			g.fieldNames[g.recordType] = goetl.GenerateColumnNames(len(result), goetl.FieldNumStyle)
		}
	}
	return result, nil
}

// Writer handles writing to a  / delimited destination
type Writer struct {
	writer     csv.Writer
	recordType string
	// writeHeader bool // TODO: figure out how I want to support this
	records int
}

// NewWriter is a constructor
func NewWriter(recordType string, wtr csv.Writer) *Writer {
	result := Writer{writer: wtr}
	return &result
}

// Write outputs the record provided to a delimited flatfile destination
func (g *Writer) Write(recordType string, record []interface{}) error {
	if recordType != g.recordType {
		return fmt.Errorf("only able to write records of type '%v'; received '%v'", g.recordType, recordType)
	}
	strrec := slicex.Stringify(record)
	err := g.writer.Write(strrec)
	g.records++
	return err
}

// RecordsWritten returnsthe number of records written
func (g *Writer) RecordsWritten() int {
	return g.records
}
