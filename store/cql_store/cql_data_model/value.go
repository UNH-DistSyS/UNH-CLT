package cql_data_model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/UNH-DistSyS/UNH-CLT/log"
)

type LocusDBValue interface {
	Serialize() []byte // this translates to []bytes
	String() string
	GetType() LocusDBType
	CastIfPossible(castType LocusDBType) (LocusDBValue, error)
	Compare(compareTo LocusDBValue) (int, error)
}

type LocusDBType uint8

const (
	Int LocusDBType = iota + 1
	BigInt
	Float
	Double
	Text
	Boolean

	//below are special reserved values
	Null       // used to designate the nil value
	Expression // expression as a string. Expression type must be evaluated before being written to store
)

func LocusDBTypeFromString(typeStr string) LocusDBType {
	typeStr = strings.ToLower(typeStr)
	switch typeStr {
	case "int":
		return Int
	case "bigint":
		return BigInt
	case "float":
		return Float
	case "double":
		return Double
	case "text":
		return Text
	case "boolean":
		return Boolean
	default:
		return Null
	}
}

func LocusDBTypeToString(ldbtype LocusDBType) string {
	switch ldbtype {
	case Int:
		return "int"
	case BigInt:
		return "bigint"
	case Float:
		return "float"
	case Double:
		return "double"
	case Text:
		return "text"
	case Boolean:
		return "boolean"
	case Null:
		return "nulltype"
	default:
		return ""
	}
}

func (f LocusDBType) GetVal() uint8 {
	return uint8(f)
}

type ComparatorType uint8

const (
	LESSTHAN = iota + 1
	LESSTHANEQUAL
	GREATERTHAN
	GREATERTHANEQUAL
	BETWEEN
	EQUAL
)

type ConstRelation struct {
	ColId uint8
	ComparatorType
	ConstVal LocusDBValue
}

func NewConstRelation(colId uint8, comparator ComparatorType, val LocusDBValue) *ConstRelation {
	return &ConstRelation{
		ColId:          colId,
		ComparatorType: comparator,
		ConstVal:       val,
	}
}

// All value types. We keep Val exported for serialization purposes to allow passing these in the messages

type IntValue struct {
	Val int32
}

type FloatValue struct {
	Val float32
}

type BigIntValue struct {
	Val int64
}

type DoubleValue struct {
	Val float64
}

type TextValue struct {
	Val string
}

type BooleanValue struct {
	Val bool
}

var NullVal = &NullValue{}

type NullValue struct{}

type ExpressionValue struct {
	Exprsn  string
	symbols map[string]LocusDBValue
}

/*-----------------------------------------------
 * Int
 *-----------------------------------------------*/

func NewIntFromReader(reader io.Reader) (*IntValue, error) {
	intBytes := make([]byte, 4)
	n, err := reader.Read(intBytes)
	if err != nil {
		log.Debugf("binary.Read failed: %v", err)
		return nil, err
	}
	if n != 4 {
		return nil, errors.New("could not read 4 bytes from stream for Int")
	}
	decodedInt := binary.BigEndian.Uint32(intBytes)
	return &IntValue{Val: int32(decodedInt)}, nil
}

func NewInt(v int32) *IntValue {
	return &IntValue{Val: int32(v)}
}

func (i IntValue) String() string {
	return fmt.Sprintf("%d", i.Val)
}

func (i IntValue) Serialize() []byte {
	intBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(intBytes, uint32(i.Val))
	return intBytes
}

func (i IntValue) GetType() LocusDBType {
	return Int
}

func (i IntValue) CastIfPossible(castType LocusDBType) (LocusDBValue, error) {
	switch castType {
	case BigInt:
		return NewBigInt(int64(i.Val)), nil
	case Int:
		return i, nil
	case Boolean:
		return NewBooleanValue(i.Val != 0), nil
	case Text:
		return NewTextValue(strconv.Itoa(int(i.Val))), nil
	case Float:
		return NewFloat(float32(i.Val)), nil
	case Double:
		return NewDoubleValue(float64(i.Val)), nil
	default:
		return nil, fmt.Errorf("cast %v -> %v is not possible for value %v", Int, castType, i)
	}
}

func (i IntValue) Compare(v2 LocusDBValue) (int, error) {
	if v2.GetType() == Int {
		v2i := v2.(IntValue)
		if i.Val > v2i.Val {
			return 1, nil
		}
		if i.Val < v2i.Val {
			return -1, nil
		}
		return 0, nil
	}
	return -1, fmt.Errorf("value to compare is not of the same type. Expecting Int, got %v", v2.GetType())
}

/*-----------------------------------------------
 * Float
 *-----------------------------------------------*/

func NewFloatValueFromReader(reader io.Reader) (*FloatValue, error) {
	var f FloatValue
	err := binary.Read(reader, binary.BigEndian, &(f.Val))
	if err != nil {
		fmt.Println("binary.Read failed:", err)
		return nil, err
	}
	return &f, nil
}

func NewFloat(f float32) *FloatValue {
	return &FloatValue{Val: f}
}

func (f FloatValue) String() string {
	return fmt.Sprintf("%f", f.Val)
}

func (f FloatValue) Serialize() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, f.Val)
	if err != nil {
		log.Debugf("binary.Write failed: %v", err)
	}
	return buf.Bytes()
}

func (f FloatValue) GetType() LocusDBType {
	return Float
}

func (f FloatValue) CastIfPossible(castType LocusDBType) (LocusDBValue, error) {
	switch castType {
	case BigInt:
		return NewBigInt(int64(f.Val)), nil
	case Int:
		return NewInt(int32(f.Val)), nil
	case Boolean:
		return NewBooleanValue(f.Val != 0), nil
	case Text:
		return NewTextValue(fmt.Sprintf("%f", f.Val)), nil
	case Float:
		return f, nil
	case Double:
		return NewDoubleValue(float64(f.Val)), nil
	default:
		return nil, fmt.Errorf("cast %v -> %v is not possible", Float, castType)
	}
}

func (i FloatValue) Compare(v2 LocusDBValue) (int, error) {
	if v2.GetType() == Float {
		v2f := v2.(FloatValue)
		if i.Val > v2f.Val {
			return 1, nil
		}
		if i.Val < v2f.Val {
			return -1, nil
		}
		return 0, nil
	}
	return -1, fmt.Errorf("value to compare is not of the same type. Expecting Float, got %v", v2.GetType())
}

/*-----------------------------------------------
 * BigInt
 *-----------------------------------------------*/

func NewBigInt(v int64) *BigIntValue {
	return &BigIntValue{Val: v}
}

func NewBigIntValueFromReader(reader io.Reader) (*BigIntValue, error) {
	intBytes := make([]byte, 8)
	n, err := reader.Read(intBytes)
	if err != nil {
		log.Debugf("binary.Read failed: %v", err)
		return nil, err
	}
	if n != 8 {
		return nil, errors.New("could not read 8 bytes from stream for BigInt")
	}
	decodedInt := binary.BigEndian.Uint64(intBytes)
	return &BigIntValue{Val: int64(decodedInt)}, nil
}

func (l BigIntValue) String() string {
	return fmt.Sprintf("%d", l.Val)
}

func (l BigIntValue) Serialize() []byte {
	intBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(intBytes, uint64(l.Val))
	return intBytes
}

func (l BigIntValue) GetType() LocusDBType {
	return BigInt
}

func (l BigIntValue) CastIfPossible(castType LocusDBType) (LocusDBValue, error) {
	switch castType {
	case BigInt:
		return l, nil
	case Int:
		return NewInt(int32(l.Val)), nil
	case Boolean:
		return NewBooleanValue(l.Val != 0), nil
	case Text:
		return NewTextValue(fmt.Sprintf("%d", l.Val)), nil
	case Float:
		return NewFloat(float32(l.Val)), nil
	case Double:
		return NewDoubleValue(float64(l.Val)), nil
	default:
		return nil, fmt.Errorf("cast %v -> %v is not possible", BigInt, castType)
	}
}

func (i BigIntValue) Compare(v2 LocusDBValue) (int, error) {
	if v2.GetType() == BigInt {
		v2i := v2.(BigIntValue)
		if i.Val > v2i.Val {
			return 1, nil
		}
		if i.Val < v2i.Val {
			return -1, nil
		}
		return 0, nil
	}
	return -1, fmt.Errorf("value to compare is not of the same type. Expecting BigInt, got %v", v2.GetType())
}

/*-----------------------------------------------
 * Double
 *-----------------------------------------------*/

func NewDoubleValue(d float64) *DoubleValue {
	return &DoubleValue{Val: d}
}

func NewDoubleValueFromReader(reader io.Reader) (*DoubleValue, error) {
	var d DoubleValue
	err := binary.Read(reader, binary.BigEndian, &(d.Val))
	if err != nil {
		fmt.Println("binary.Read failed:", err)
		return nil, err
	}
	return &d, nil
}

func (d DoubleValue) String() string {
	return fmt.Sprintf("%f", d.Val)
}

func (d DoubleValue) Serialize() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, d.Val)
	if err != nil {
		log.Debugf("binary.Write failed: %v", err)
	}
	return buf.Bytes()
}

func (d DoubleValue) GetType() LocusDBType {
	return Double
}

func (d DoubleValue) CastIfPossible(castType LocusDBType) (LocusDBValue, error) {
	switch castType {
	case BigInt:
		return NewBigInt(int64(d.Val)), nil
	case Int:
		return NewInt(int32(d.Val)), nil
	case Boolean:
		return NewBooleanValue(d.Val != 0), nil
	case Text:
		return NewTextValue(fmt.Sprintf("%f", d.Val)), nil
	case Float:
		return NewFloat(float32(d.Val)), nil
	case Double:
		return d, nil
	default:
		return nil, fmt.Errorf("cast %v -> %v is not possible", Double, castType)
	}
}

func (d DoubleValue) Compare(v2 LocusDBValue) (int, error) {
	if v2.GetType() == Double {
		v2i := v2.(DoubleValue)
		if d.Val > v2i.Val {
			return 1, nil
		}
		if d.Val < v2i.Val {
			return -1, nil
		}
		return 0, nil
	}
	return -1, fmt.Errorf("value to compare is not of the same type. Expecting Double, got %v", v2.GetType())
}

/*-----------------------------------------------
 * Text
 *-----------------------------------------------*/

func NewTextValue(s string) *TextValue {
	return &TextValue{Val: s}
}

func (s TextValue) String() string {
	return fmt.Sprintf(s.Val)
}

func NewTextValueFromReader(reader io.Reader) (*TextValue, error) {
	buf := make([]byte, 4)
	_, err := reader.Read(buf)
	if err != nil {
		fmt.Println("Failed to read string length in bytes:", err)
		return nil, err
	}
	strLenBytes := binary.BigEndian.Uint32(buf)

	buf = make([]byte, strLenBytes)
	_, err = reader.Read(buf)
	if err != nil {
		fmt.Println("Failed to read string from buffer", err)
		return nil, err
	}

	return &TextValue{Val: string(buf)}, nil
}

func (s TextValue) Serialize() []byte {
	bufBytes := make([]byte, len(s.Val)+4)
	binary.BigEndian.PutUint32(bufBytes, uint32(len(s.Val)))

	textBytes := []byte(s.Val)
	copy(bufBytes[4:], textBytes)
	return bufBytes

	/*buf := bytes.NewBuffer(bufBytes)
	err := binary.Write(buf, binary.BigEndian, int32(len(s.Val)))
	if err != nil {
		log.Debugf("binary.Write failed: %v", err)
	}

	err = binary.Write(buf, binary.BigEndian, []byte(s.Val))
	if err != nil {
		log.Debugf("binary.Write failed: %v", err)
	}

	return buf.Bytes()*/
}

func (s TextValue) GetType() LocusDBType {
	return Text
}

func (s TextValue) CastIfPossible(castType LocusDBType) (LocusDBValue, error) {
	switch castType {
	case Int:
		iVal, err := strconv.Atoi(s.Val)
		if err != nil {
			return nil, err
		}
		return NewInt(int32(iVal)), nil
	case BigInt:
		iVal, err := strconv.ParseInt(s.Val, 10, 64)
		if err != nil {
			return nil, err
		}
		return NewBigInt(iVal), nil
	case Float:
		fVal, err := strconv.ParseFloat(s.Val, 32)
		if err != nil {
			return nil, err
		}
		return NewFloat(float32(fVal)), nil
	case Double:
		fVal, err := strconv.ParseFloat(s.Val, 64)
		if err != nil {
			return nil, err
		}
		return NewDoubleValue(fVal), nil
	case Boolean:
		bVal, err := strconv.ParseBool(s.Val)
		if err != nil {
			return nil, err
		}
		return NewBooleanValue(bVal), nil
	case Text:
		return s, nil
	default:
		return nil, fmt.Errorf("cast %v -> %v is not possible", Text, castType)
	}
}

func (t TextValue) Compare(v2 LocusDBValue) (int, error) {
	if v2.GetType() == Text {
		v2i := v2.(TextValue)
		if t.Val > v2i.Val {
			return 1, nil
		}
		if t.Val < v2i.Val {
			return -1, nil
		}
		return 0, nil
	}
	return -1, fmt.Errorf("value to compare is not of the same type. Expecting Text, got %v", v2.GetType())
}

/*-----------------------------------------------
 * Boolean
 *-----------------------------------------------*/
func NewBooleanValue(b bool) *BooleanValue {
	return &BooleanValue{Val: b}
}

func NewBooleanValueFromReader(reader io.Reader) (*BooleanValue, error) {
	var b BooleanValue
	err := binary.Read(reader, binary.BigEndian, &(b.Val))
	if err != nil {
		fmt.Println("binary.Read failed:", err)
		return nil, err
	}
	return &b, nil
}

func (b BooleanValue) String() string {
	return fmt.Sprintf("%t", b.Val)
}

func (b BooleanValue) Serialize() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, b.Val)
	if err != nil {
		log.Debugf("binary.Write failed: %v", err)
	}
	return buf.Bytes()
}

func (b BooleanValue) GetType() LocusDBType {
	return Boolean
}

func (b BooleanValue) CastIfPossible(castType LocusDBType) (LocusDBValue, error) {
	switch castType {
	case BigInt:
		if b.Val {
			return NewBigInt(1), nil
		} else {
			return NewBigInt(0), nil
		}
	case Int:
		if b.Val {
			return NewInt(1), nil
		} else {
			return NewInt(0), nil
		}
	case Boolean:
		return b, nil
	case Text:
		return NewTextValue(fmt.Sprintf("%t", b.Val)), nil
	case Float:
		if b.Val {
			return NewFloat(1), nil
		} else {
			return NewFloat(0), nil
		}
	case Double:
		if b.Val {
			return NewDoubleValue(1), nil
		} else {
			return NewDoubleValue(0), nil
		}
	default:
		return nil, fmt.Errorf("cast %v -> %v is not possible", Boolean, castType)
	}
}

func (b BooleanValue) Compare(v2 LocusDBValue) (int, error) {
	if v2.GetType() == Boolean {
		v2i := v2.(BooleanValue)
		if b.Val == v2i.Val {
			return 0, nil
		}

		return 1, nil
	}
	return -1, fmt.Errorf("value to compare is not of the same type. Expecting Boolean, got %v", v2.GetType())
}

/*-----------------------------------------------
 * NullValue
 *-----------------------------------------------*/
func NewNullValue() *NullValue {
	return &NullValue{}
}

func (n NullValue) String() string {
	return "NULL"
}

func (n NullValue) Serialize() []byte {
	return nil
}

func (n NullValue) GetType() LocusDBType {
	return Null
}

func (n NullValue) CastIfPossible(castType LocusDBType) (LocusDBValue, error) {
	switch castType {
	case Null:
		return n, nil
	default:
		return nil, fmt.Errorf("cast %v -> %v is not possible", Null, castType)
	}
}

func (n NullValue) Compare(v2 LocusDBValue) (int, error) {
	if n == v2 {
		return 0, nil
	}

	if v2.GetType() == Null {
		return 0, nil
	}
	return -1, fmt.Errorf("value to compare is not of the same type. Expecting Null, got %v", v2.GetType())
}

/*-----------------------------------------------
 * ExpressionValue
 * We use the expression value when translating
 * some operations where the value depends on the
 * other columns or other queries (i.e. a readset)
 *-----------------------------------------------*/
func NewExpressionValue(exprsn string) *ExpressionValue {
	return &ExpressionValue{Exprsn: exprsn}
}

func (ev *ExpressionValue) SetSymbolTable(symbols map[string]LocusDBValue) {
	ev.symbols = symbols
}

func (ev ExpressionValue) String() string {
	return ev.Exprsn
}

func (ev ExpressionValue) Serialize() []byte {
	return nil
}

func (ev ExpressionValue) GetType() LocusDBType {
	return Expression
}

func (ev ExpressionValue) CastIfPossible(castType LocusDBType) (LocusDBValue, error) {
	switch castType {
	default:
		return nil, fmt.Errorf("cast %v -> %v is not possible for value %v", Expression, castType, ev.String())
	}
}

func (ev ExpressionValue) Compare(v2 LocusDBValue) (int, error) {
	if v2.GetType() == Expression {
		v2e := v2.(ExpressionValue)
		if ev.Exprsn > v2e.Exprsn {
			return 1, nil
		}
		if ev.Exprsn < v2e.Exprsn {
			return -1, nil
		}
		return 0, nil
	}
	return -1, fmt.Errorf("value to compare is not of the same type. Expecting Expression, got %v", v2.GetType())
}
