//go:build ignore

package main

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"go/format"

	"github.com/fission-codes/go-car-mirror/carmirror"
)

func main() {

	if len(os.Args) != 3 {
		fmt.Println("Usage: go run gen.go <struct name> <output file>")
		os.Exit(1)
	}
	structName := os.Args[1]
	newStructName := "Instrumented" + structName
	outputFile := os.Args[2]

	fmt.Printf("Writing struct %s to file %s.\n", newStructName, outputFile)

	var code strings.Builder

	// Can't find a way to go from string to struct reference, without building my own registry.
	structNameToInstance := map[string]interface{}{
		"BatchSendOrchestrator":    &carmirror.BatchSendOrchestrator{},
		"BatchReceiveOrchestrator": &carmirror.BatchReceiveOrchestrator{},
	}
	s, ok := structNameToInstance[structName]
	if !ok {
		fmt.Println("Unknown struct name:", structName)
		os.Exit(1)
	}

	// We have to use a pointer here, or else we won't get any methods.
	t := reflect.TypeOf(s)

	fmt.Fprintln(&code, "package carmirror")
	fmt.Fprintln(&code)

	// Generate the instrumented struct.
	// Note that since t is a pointer, we need to use t.Elem() to get the underlying type.
	// If t was not a pointer, we could use t directly.
	fmt.Fprintf(&code, "type %s struct {\n", newStructName)
	fmt.Fprintf(&code, "	original *%s\n", t.Elem().Name())
	fmt.Fprintf(&code, "	stats Stats\n")
	fmt.Fprintf(&code, "}\n")

	fmt.Fprintln(&code)

	// Generate the New method.
	fmt.Fprintf(&code, "func New%s(original *%s, stats Stats) *%s {\n", newStructName, t.Elem().Name(), newStructName)
	fmt.Fprintf(&code, "	return &%s{\n", newStructName)
	fmt.Fprintf(&code, "		original: original,\n")
	fmt.Fprintf(&code, "		stats: stats,\n")
	fmt.Fprintf(&code, "	}\n")
	fmt.Fprintf(&code, "}\n")

	fmt.Fprintln(&code)

	// Generate the instrumented methods.
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)

		fmt.Fprintf(&code, "func (i *%s) %s(", newStructName, method.Name)
		mType := method.Type
		for j := 1; j < mType.NumIn(); j++ {
			param := mType.In(j)
			fmt.Fprintf(&code, "%s %s", lowerFirstLetter(param.Name()), param.Name())
			if j < mType.NumIn()-1 {
				fmt.Fprintf(&code, ", ")
			}
		}
		fmt.Fprintf(&code, ")")
		if mType.NumOut() > 0 {
			fmt.Fprintf(&code, " ")
			for j := 0; j < mType.NumOut(); j++ {
				ret := mType.Out(j)
				fmt.Fprintf(&code, "%s", ret.Name())
				if j < mType.NumOut()-1 {
					fmt.Fprintf(&code, ", ")
				}
			}
		}
		fmt.Fprintf(&code, " {\n")
		fmt.Fprintf(&code, "	i.stats.Logger().Debugw(\"%s\", \"method\", \"%s\")\n", newStructName, method.Name)
		fmt.Fprintf(&code, "	return i.original.%s(", method.Name)
		for j := 1; j < mType.NumIn(); j++ {
			param := mType.In(j)
			fmt.Fprintf(&code, "%s", lowerFirstLetter(param.Name()))
			if j < mType.NumIn()-1 {
				fmt.Fprintf(&code, ", ")
			}
		}
		fmt.Fprintf(&code, ")\n")
		fmt.Fprintf(&code, "}\n")

		fmt.Fprintln(&code)
	}

	// fmt.Printf("%s", code.String())

	formattedCode, err := format.Source([]byte(code.String()))
	if err != nil {
		fmt.Println("Error formatting code:", err)
		return
	}

	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer out.Close()

	fmt.Fprintf(out, "%s", formattedCode)
}

func lowerFirstLetter(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}
