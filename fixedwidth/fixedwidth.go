package fixedwidth

import (
	"errors"
	"fmt"
	"io"

	"github.com/mattmc3/goetl"
	"github.com/mattmc3/gofurther/slicex"
	"github.com/mattmc3/gofurther/stringsx"
)

// Formatter defines a function signature for formatting fixed width
// values when writing them
type Formatter func(fwdef *FieldDef, value string) (string, error)

// ErrTruncatedValue indicates a value was too long and required truncation
var ErrTruncatedValue = errors.New("the value is too long and must be truncated")

// FormatLeft adds space padding to the right of the specified value and
// truncates the data if it exceeds the field length. If the data was truncated,
// an ErrTruncatedValue error is returned.
func FormatLeft(fwdef *FieldDef, value string) (string, error) {
	if fwdef.Length < len(value) {
		if fwdef.Length <= 0 {
			return "", ErrTruncatedValue
		}
		return value[:fwdef.Length], ErrTruncatedValue
	}
	return stringsx.AlignLeft(value, ' ', fwdef.Length), nil
}

// FormatRight adds space padding to the left of the specified value and
// truncates the data if it exceeds the field length. If the data was truncated,
// an ErrTruncatedValue error is returned.
func FormatRight(fwdef *FieldDef, value string) (string, error) {
	if fwdef.Length < len(value) {
		if fwdef.Length <= 0 {
			return "", ErrTruncatedValue
		}
		return value[:fwdef.Length], ErrTruncatedValue
	}
	return stringsx.AlignRight(value, ' ', fwdef.Length), nil
}

// FieldDef represents a fixed width field definition
type FieldDef struct {
	FieldName string
	Offset    int
	Length    int
	Formatter Formatter
}

// FormatValue takes a value and applies the formatter for the
// FieldDef to that value.
func (g *FieldDef) FormatValue(value string) (string, error) {
	if g.Formatter == nil {
		return value, nil
	}
	return g.Formatter(g, value)
}

// NewFieldDef constructor
func NewFieldDef(fieldName string, offset int, length int) *FieldDef {
	result := FieldDef{
		FieldName: fieldName,
		Offset:    offset,
		Length:    length,
	}
	return &result
}

// GenerateFieldDefs returns a field def array representing the
// field definitions by name and field length
func GenerateFieldDefs(fieldNames []string, fieldLengths []int) ([]FieldDef, error) {
	if len(fieldLengths) != len(fieldNames) {
		return nil, errors.New("number of field names and lengths differ")
	} else if len(fieldNames) == 0 {
		return nil, errors.New("no field names and lengths specified")
	}

	var fwfields = make([]FieldDef, len(fieldNames))
	offset := 0
	for i, l := range fieldLengths {
		if l < 0 {
			l = 0
		}
		fld := FieldDef{
			FieldName: fieldNames[i],
			Offset:    offset,
			Length:    l,
		}
		fwfields[i] = fld
		offset += l
	}
	return fwfields, nil
}

// Reader handles reading from a fixed width file.
type Reader struct {
	reader       io.Reader
	Fields       []FieldDef
	recordType   string
	recordNumber int
}

// NewReader constructor
func NewReader(recordType string, rdr io.Reader, fields []FieldDef) *Reader {
	result := Reader{
		reader:     rdr,
		Fields:     fields,
		recordType: recordType,
	}
	return &result
}

// NewReaderByLengths constructor
func NewReaderByLengths(recordType string, rdr io.Reader, fieldLengths []int) *Reader {
	var fwfields = make([]FieldDef, len(fieldLengths))
	offset := 0
	for i, l := range fieldLengths {
		if l < 0 {
			l = 0
		}
		fld := FieldDef{
			FieldName: goetl.FieldNumStyle(i),
			Offset:    offset,
			Length:    l,
		}
		fwfields[i] = fld
	}
	return NewReader(recordType, rdr, fwfields)
}

// ReadNext reads the next record if one is available. When the end of the data
// set is reached, the EndOfRecords error is returned.
// func (g *Reader) ReadNext() ([]interface{}, error) {
// 	idx := &g.recordNumber
// 	*idx++

// 	var rec []string
// 	var result []interface{}
// 	var err error

// 	rec, err = g.reader.Read()
// 	result = slicex.ObjectifyStrings(rec)
// 	if err != nil {
// 		if err == io.EOF {
// 			return nil, EndOfRecords
// 		}
// 		return result, err
// 	}

// 	if *idx == 1 {
// 		if g.hasHeader {
// 			g.fieldNames[g.recordType] = rec
// 		} else {
// 			g.fieldNames[g.recordType] = GenerateColumnNames(len(result), ColNumStyle)
// 		}
// 	}
// 	return result, nil
// }

// Writer handles writing to a fixed width destination
type Writer struct {
	writer     io.Writer
	recordType string
	records    int
}

// NewWriter is a constructor
func NewWriter(recordType string, wtr io.Writer) *Writer {
	result := Writer{writer: wtr}
	return &result
}

// Write outputs the record provided to a fixed width flatfile destination
func (g *Writer) Write(recordType string, record []interface{}) error {
	if recordType != g.recordType {
		return fmt.Errorf("only able to write records of type '%v'; received '%v'", g.recordType, recordType)
	}
	byterecs := slicex.Byteify(record)

	var err error
	write := func(buf []byte) {
		if err != nil {
			return
		}
		_, err = g.writer.Write(buf)
	}

	for _, buf := range byterecs {
		write(buf)
	}
	return err
}

// RecordsWritten returns the total number of records added by the writer.
func (g *Writer) RecordsWritten() int {
	return g.records
}
