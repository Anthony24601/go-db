package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

var temp_int int64 = 0
var temp_char byte = 'a'

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

func (t DBType) String() string {
	switch t {
	case IntType:
		return "int"
	case StringType:
		return "string"
	}
	return "unknown"
}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	if len(d1.Fields) != len(d2.Fields) {
		return false
	} else {
		// If statement above prevents errors for uneven field lengths
		for i, f := range d1.Fields {
			if f != d2.Fields[i] {
				return false
			}
		}
		return true
	}
}

// Hint: heap_page need function there:  (desc *TupleDesc) bytesPerTuple() int
// Calculates the size of the tuple
func (desc *TupleDesc) bytesPerTuple() int {
	size := 0
	for i := 0; i < len(desc.Fields); i++ {
		if desc.Fields[i].Ftype == IntType {
			size += (int)(unsafe.Sizeof(temp_int))
		} else if desc.Fields[i].Ftype == StringType {
			size += ((int)(unsafe.Sizeof(temp_char))) * StringLength
		} // No else, maybe we want to throw error here
	}
	return size
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	return &TupleDesc{fields}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// ... is so that the slice gets added one element at a time rather than the slice as a single element
	return &TupleDesc{append(desc.Fields, desc2.Fields...)}
}

// ================== Tuple Methods ======================

// Interface for tuple field values
type DBValue interface {
	EvalPred(v DBValue, op BoolOp) bool
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian order.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	for i := 0; i < len(t.Fields); i++ {
		tuple_field := t.Fields[i]
		switch tuple_field := tuple_field.(type) { // Couldn't figure out how to use If statement here since IntField and StringField are special
		case IntField:
			err := binary.Write(b, binary.LittleEndian, tuple_field.Value)
			if err != nil {
				return err
			}
		case StringField:
			err := binary.Write(b, binary.LittleEndian, []byte(tuple_field.Value))
			if err != nil {
				return err
			}
			rest := StringLength - len(tuple_field.Value)                  // Gets how many things we need to pad with
			err = binary.Write(b, binary.LittleEndian, make([]byte, rest)) // Pad
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	outFields := make([]DBValue, len(desc.Fields))
	for i := 0; i < len(desc.Fields); i++ {
		switch desc.Fields[i].Ftype {
		case IntType:
			var inInt int64
			err := binary.Read(b, binary.LittleEndian, &inInt)
			if err != nil {
				return nil, err
			}
			outFields[i] = IntField{inInt} // add to field
		case StringType:
			bin_str := make([]byte, StringLength)
			err := binary.Read(b, binary.LittleEndian, bin_str)
			if err != nil {
				return nil, err
			}
			var str = string(bytes.TrimRight(bin_str, "\x00")) // Trim padding
			outFields[i] = StringField{str}                    // add to field
		}
	}

	return &Tuple{*desc, outFields, nil}, nil
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	if !t1.Desc.equals(&t2.Desc) {
		return false
	}
	for i, f := range t1.Fields {
		if f != t2.Fields[i] {
			return false
		}
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2
// appended to t1. The new tuple should have a correct TupleDesc that is created
// by merging the descriptions of the two input tuples.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	if t1 == nil { // If nil then we don't need to join anything
		return t2
	} else if t2 == nil {
		return t1
	} else {
		var desc_temp = t1.Desc.merge(&t2.Desc)
		var field_temp = append(t1.Fields, t2.Fields...)
		return &Tuple{*desc_temp, field_temp, nil}
	}
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
//
// Note that EvalExpr uses the [Tuple.project] method, so you will need
// to implement projection before testing compareField.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	first_field, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, err
	}
	second_field, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err
	}

	if field.GetExprType().Ftype == IntType {
		first_val := first_field.(IntField).Value
		second_val := second_field.(IntField).Value
		if first_val < second_val { // Can't modularize this I don't think :(
			return OrderedLessThan, nil
		} else if first_val == second_val {
			return OrderedEqual, nil
		} else {
			return OrderedGreaterThan, nil
		}
	} else if field.GetExprType().Ftype == StringType {
		first_val := first_field.(StringField).Value
		second_val := second_field.(StringField).Value
		if first_val < second_val { // Can't modularize this I don't think :(
			return OrderedLessThan, nil
		} else if first_val == second_val {
			return OrderedEqual, nil
		} else {
			return OrderedGreaterThan, nil
		}
	}
	// It doesn't matter what we return in first spot here I think
	return OrderedEqual, GoDBError{IncompatibleTypesError, "Not IntType or StringType"}
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	values := make([]DBValue, len(fields))
	idx := 0
	for _, fType := range fields {
		proj_ind, err := findFieldInTd(fType, &t.Desc)
		if err != nil {
			return nil, err
		} else {
			values[idx] = t.Fields[proj_ind]
			idx += 1
		}
	}
	return &Tuple{TupleDesc{fields}, values, nil}, nil
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {
	var buf bytes.Buffer
	t.writeTo(&buf)
	return buf.String()
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = strconv.FormatInt(f.Value, 10)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr
}
