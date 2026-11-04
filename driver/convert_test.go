package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

type valueValuer string

func (v valueValuer) Value() (driver.Value, error) {
	return string(v), nil
}

type nilAwareValuer struct{}

func (*nilAwareValuer) Value() (driver.Value, error) {
	return "nil value", nil
}

type errorValuer struct{}

func (*errorValuer) Value() (driver.Value, error) {
	return nil, io.ErrClosedPipe
}

// TestValuerValue checks that nil pointers with value-receiver methods become
// SQL NULL, while nil-aware pointer-receiver methods retain their value or error.
func TestValuerValue(t *testing.T) {
	t.Parallel()

	value := valueValuer("value")
	for _, test := range []struct {
		name    string
		value   driver.Valuer
		want    driver.Value
		wantErr error
	}{
		{"nil value receiver", (*valueValuer)(nil), nil, nil},
		{"value receiver", value, "value", nil},
		{"value receiver pointer", &value, "value", nil},
		{"nil pointer receiver", (*nilAwareValuer)(nil), "nil value", nil},
		{"nil pointer receiver error", (*errorValuer)(nil), nil, io.ErrClosedPipe},
		{"nil generic Null", (*sql.Null[Decimal])(nil), nil, nil},
		{"generic Null", sql.Null[int64]{V: 42, Valid: true}, int64(42), nil},
	} {
		got, err := valuerValue(test.value)
		if !errors.Is(err, test.wantErr) {
			t.Errorf("%s: got error %v, want %v", test.name, err, test.wantErr)
		}
		if got != test.want {
			t.Errorf("%s: got %v, want %v", test.name, got, test.want)
		}
	}
}
