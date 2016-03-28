package datasource

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/mattmc3/gofurther/slicex"
)

// CSVReader handles reading from a delimited flatfile or csv stream.
type CSVReader struct {
	reader    *csv.Reader
	hasHeader bool
	BaseReader
}

// NewCSVReader is a constructor
func NewCSVReader(recordType string, rdr *csv.Reader, hasHeader bool) *CSVReader {
	fields := make(map[string][]string)
	result := CSVReader{
		reader:    rdr,
		hasHeader: hasHeader,
		BaseReader: BaseReader{
			fields:     fields,
			recordType: recordType,
		},
	}
	return &result
}

// ReadNext reads the next record if one is available. When the end of the data
// set is reached, the EndOfRecords error is returned.
func (g *CSVReader) ReadNext() ([]interface{}, error) {
	idx := &g.recordNumber
	*idx++

	var rec []string
	var result []interface{}
	var err error

	rec, err = g.reader.Read()
	result = slicex.ObjectifyStrings(rec)
	if err != nil {
		if err == io.EOF {
			return nil, EndOfRecords
		}
		return result, err
	}

	if *idx == 1 {
		if g.hasHeader {
			g.fields[g.recordType] = rec
		} else {
			g.fields[g.recordType] = GenerateDefaultFieldNames(len(result))
		}
	}
	return result, nil
}

// CSVWriter handles writing to a CSV / delimited destination
type CSVWriter struct {
	writer     csv.Writer
	recordType string
	// writeHeader bool // TODO: figure out how I want to support this
	BaseWriter
}

// NewCSVWriter is a constructor
func NewCSVWriter(recordType string, wtr csv.Writer) *CSVWriter {
	result := CSVWriter{writer: wtr}
	return &result
}

// Write outputs the record provided to a delimited flatfile destination
func (g *CSVWriter) Write(recordType string, record []interface{}) error {
	if recordType != g.recordType {
		return fmt.Errorf("only able to write records of type '%v'; received '%v'", g.recordType, recordType)
	}
	strrec := slicex.Stringify(record)
	err := g.writer.Write(strrec)
	return err
}
