//go:build debug

package dynamo

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
)

// debugging tools for dynamo developers
// disabled by default, use `go build -tags debug` to enable
// warning: not covered by API stability guarantees

// DumpType dumps a description of x's typedef to stdout.
func DumpType(x any) {
	plan, err := typedefOf(reflect.TypeOf(x))
	if err != nil {
		panic(err)
	}
	plan.dump()
}

func (plan *typedef) dumpDecoders() []unmarshalKey {
	keys := make([]unmarshalKey, 0, len(plan.decoders))
	for key := range plan.decoders {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Less(keys[j])
	})
	return keys
}

func (plan *typedef) dump() {
	funcname := func(f any) string {
		name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		if name == "" {
			return "<nil>"
		}
		return strings.TrimPrefix(name, "github.com/guregu/")
	}

	fmt.Fprintf(os.Stdout, "DECODERS (%d)\n", len(plan.decoders))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	for _, key := range plan.dumpDecoders() {
		fn := plan.decoders[key]
		fmt.Fprintf(w, "%#v\t->\t%v\n", key, funcname(fn))
	}
	w.Flush()
	fmt.Println()

	w = tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(os.Stdout, "ENCODERS (%d)\n", len(plan.fields))
	for _, field := range plan.fields {
		fmt.Fprintf(w, "%s\t%v\t->\t%v\t%v\n", field.name, field.index, funcname(field.enc), funcname(field.isZero))
	}
	w.Flush()
}

// func debugf(format string, args ...any) {
// 	for i := range args {
// 		rv := reflect.ValueOf(args[i])
// 		if rv.Kind() == reflect.Func {
// 			args[i] = runtime.FuncForPC(rv.Pointer()).Name()
// 		}
// 	}
// 	fmt.Println("・", fmt.Sprintf(format, args...))
// }
