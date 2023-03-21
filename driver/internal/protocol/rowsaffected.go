package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

// rows affected
const (
	raSuccessNoInfo   = -2
	RaExecutionFailed = -3
)

// RowsAffected represents a rows affected part.
type RowsAffected struct {
	Ofs  int
	rows []int32
}

func (r RowsAffected) String() string {
	return fmt.Sprintf("%v", r.rows)
}

func (r *RowsAffected) decode(dec *encoding.Decoder, ph *PartHeader) error {
	r.rows = resizeSlice(r.rows, ph.numArg())

	for i := 0; i < ph.numArg(); i++ {
		r.rows[i] = dec.Int32()
	}
	return dec.Error()
}

// Total return the total number of all affected rows.
func (r RowsAffected) Total() int64 {
	total := int64(0)
	for _, rows := range r.rows {
		if rows > 0 {
			total += int64(rows)
		}
	}
	return total
}
