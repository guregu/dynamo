package dynamo

import "reflect"

type encodeFlags uint

const (
	flagSet encodeFlags = 1 << iota
	flagOmitEmpty
	flagOmitEmptyElem
	flagAllowEmpty
	flagAllowEmptyElem
	flagNull
	flagUnixTime

	flagNone encodeFlags = 0
)

func fieldInfo(field reflect.StructField) (name string, flags encodeFlags) {
	tag := field.Tag.Get("dynamo")
	if tag == "" {
		return field.Name, flagNone
	}

	begin := 0
	for i := 0; i <= len(tag); i++ {
		if !(i == len(tag) || tag[i] == ',') {
			continue
		}
		part := tag[begin:i]
		begin = i + 1

		if name == "" {
			if part == "" {
				name = field.Name
			} else {
				name = part
			}
			continue
		}

		switch part {
		case "set":
			flags |= flagSet
		case "omitempty":
			flags |= flagOmitEmpty
		case "omitemptyelem":
			flags |= flagOmitEmptyElem
		case "allowempty":
			flags |= flagAllowEmpty
		case "allowemptyelem":
			flags |= flagAllowEmptyElem
		case "null":
			flags |= flagNull
		case "unixtime":
			flags |= flagUnixTime
		}
	}

	return
}
