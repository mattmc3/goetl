package etl

import (
	"github.com/mattmc3/goetl/record"
)

// PushData reads all records from the source and writes them to the
// destination. Reader or Writer errors are returned if they occur.
func PushData(source record.Reader, dest record.Writer) error {
	for {
		rec, err := source.ReadNext()
		if err != nil {
			if err == record.EndOfRecords {
				break
			} else {
				return err
			}
		}
		err = dest.Write(source.RecordType(), rec)
		if err != nil {
			return err
		}
	}
	return nil
}
