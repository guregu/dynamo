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

// TODO: delete me
func DumpType(x any) {
	plan, err := getDecodePlan(reflect.TypeOf(x))
	if err != nil {
		panic(err)
	}
	plan.dump()
}

func (plan *decodePlan) dumpDecoders() []unmarshalKey {
	keys := make([]unmarshalKey, 0, len(plan.decoders))
	for key := range plan.decoders {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Less(keys[j])
	})
	return keys
}

func (plan *decodePlan) dump() {
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
		fmt.Fprintf(w, "%s\t%v\t->\t%v\n", field.name, field.index, funcname(field.enc))
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
