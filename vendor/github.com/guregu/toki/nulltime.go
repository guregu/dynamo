package toki

import (
	"database/sql/driver"
)

// NullTime is a nullable Time.
type NullTime struct {
	Time
	Valid bool
}

// UnmarshalText implements the encoding TextUnmarshaler interface.
// Empty strings and "null" will be considered null.
func (t *NullTime) UnmarshalText(text []byte) error {
	t.Valid = true
	str := string(text)
	if str == "" || str == "null" {
		t.Valid = false
		return nil
	}
	return t.Time.UnmarshalText(text)
}

// UnmarshalJSON implements the JSON Unmarshaler interface.
// Empty strings and "null" will be considered null.
func (t *NullTime) UnmarshalJSON(data []byte) error {
	t.Valid = true
	text := string(data)
	if text == `""` || text == "null" {
		t.Valid = false
		return nil
	}
	return t.UnmarshalText(data[1 : len(data)-1])
}

// Scan implements the driver Scanner interface.
func (t *NullTime) Scan(src interface{}) error {
	t.Valid = true
	switch x := src.(type) {
	case []byte:
		if len(x) == 0 {
			t.Valid = false
			return nil
		}
	case string:
		if x == "" || x == "null" {
			t.Valid = false
			return nil
		}
	case nil:
		t.Valid = false
		return nil
	}
	return t.Time.Scan(src)
}

// MarshalText implements the encoding TextMarshaler interface.
// Encodes to hh:mm:ss and omits the seconds if 0.
// Encodes to an empty string if null.
func (t NullTime) MarshalText() (text []byte, err error) {
	if !t.Valid {
		return []byte{}, nil
	}
	return t.Time.MarshalText()
}

// MarshalJSON implements the JSON Marshaler interface.
// Encodes to hh:mm:ss and omits the seconds if 0.
// Encodes to null if null.
func (t NullTime) MarshalJSON() ([]byte, error) {
	if !t.Valid {
		return []byte("null"), nil
	}
	text, _ := t.MarshalText()
	// what is the best way to do this?
	out := make([]byte, 0, len(text)+2)
	out = append(out, '"')
	out = append(out, text...)
	out = append(out, '"')
	return out, nil
}

// Value implements the driver Valuer interface.
func (t NullTime) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	}
	return t.Time.MarshalText()
}

// String returns a string representation of this Time.
func (t NullTime) String() string {
	text, _ := t.MarshalText()
	return string(text)
}

// Equals returns true if this Time and the given Time are equal.
// If they are both null it will return true.
func (t NullTime) Equals(other NullTime) bool {
	switch {
	case !t.Valid && !other.Valid:
		return true
	case t.Valid != other.Valid:
		return false
	}
	return t.Time.Equals(other.Time)
}

// ParseNullTime tries to parse the given time.
func ParseNullTime(text string) (NullTime, error) {
	t := &NullTime{}
	err := t.UnmarshalText([]byte(text))
	return *t, err
}

// MustParseNullTime parses the given time or panics.
func MustParseNullTime(text string) NullTime {
	t, err := ParseNullTime(text)
	if err != nil {
		panic(err)
	}
	return t
}
