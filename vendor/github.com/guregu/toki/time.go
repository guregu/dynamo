package toki

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Time represents an SQL TIME value.
type Time struct {
	Hours   int
	Minutes int
	Seconds int
}

// UnmarshalText implements the encoding TextUnmarshaler interface.
func (t *Time) UnmarshalText(text []byte) error {
	parts := strings.Split(string(text), ":")
loop:
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return err
		}
		switch i {
		case 0:
			t.Hours = n
		case 1:
			t.Minutes = n
		case 2:
			t.Seconds = n
		default:
			break loop
		}
	}
	return nil
}

// Scan implements the driver Scanner interface.
func (t *Time) Scan(src interface{}) error {
	switch x := src.(type) {
	case []byte:
		return t.UnmarshalText(x)
	case string:
		return t.UnmarshalText([]byte(x))
	case time.Time:
		t.Hours = x.Hour()
		t.Minutes = x.Minute()
		t.Seconds = x.Second()
		return nil
	}
	return fmt.Errorf("unsupported type: %T", src)
}

// MarshalText implements the encoding TextMarshaler interface.
// Encodes to hh:mm:ss and omits the seconds if 0.
func (t Time) MarshalText() (text []byte, err error) {
	var buf bytes.Buffer
	buf.WriteString(strconv.Itoa(t.Hours))
	buf.WriteByte(':')
	if t.Minutes < 10 {
		buf.WriteByte('0')
	}
	buf.WriteString(strconv.Itoa(t.Minutes))
	if t.Seconds != 0 {
		buf.WriteByte(':')
		if t.Seconds < 10 {
			buf.WriteByte('0')
		}
		buf.WriteString(strconv.Itoa(t.Seconds))
	}
	return buf.Bytes(), nil
}

// Value implements the driver Valuer interface.
func (t Time) Value() (driver.Value, error) {
	return t.MarshalText()
}

// String returns a string representation of this Time.
func (t Time) String() string {
	text, _ := t.MarshalText()
	return string(text)
}

// Equals returns true if this Time and the given Time are equal.
// If they are both null it will return true.
func (t Time) Equals(other Time) bool {
	return t.Hours == other.Hours &&
		t.Minutes == other.Minutes &&
		t.Seconds == other.Seconds
}

// ParseTime tries to parse the given time.
func ParseTime(text string) (Time, error) {
	t := &Time{}
	err := t.UnmarshalText([]byte(text))
	return *t, err
}

// MustParseTime parses the given time or panics.
func MustParseTime(text string) Time {
	t, err := ParseTime(text)
	if err != nil {
		panic(err)
	}
	return t
}
