package resp3

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	. "testing"

	"errors"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeekAndAssertPrefix(t *T) {
	type test struct {
		in, prefix []byte
		exp        error
	}

	tests := []test{
		{[]byte(":5\r\n"), NumberPrefix, nil},
		{[]byte(":5\r\n"), SimpleStringPrefix, resp.ErrConnUsable{
			Err: errUnexpectedPrefix{
				Prefix: NumberPrefix, ExpectedPrefix: SimpleStringPrefix,
			},
		}},
		{[]byte("-foo\r\n"), SimpleErrorPrefix, nil},
		// TODO BlobErrorPrefix
		{[]byte("-foo\r\n"), NumberPrefix, resp.ErrConnUsable{Err: SimpleError{
			S: "foo",
		}}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			br := bufio.NewReader(bytes.NewReader(test.in))
			err := peekAndAssertPrefix(br, test.prefix, false)

			assert.IsType(t, test.exp, err)
			if expUsable, ok := test.exp.(resp.ErrConnUsable); ok {
				usable, _ := err.(resp.ErrConnUsable)
				assert.IsType(t, expUsable.Err, usable.Err)
			}
			if test.exp != nil {
				assert.Equal(t, test.exp.Error(), err.Error())
			}
		})
	}
}

func TestRESPTypes(t *T) {
	// TODO only used by BulkReader test
	//newLR := func(s string) resp.LenReader {
	//	buf := bytes.NewBufferString(s)
	//	return resp.NewLenReader(buf, int64(buf.Len()))
	//}

	newBigInt := func(s string) *big.Int {
		i, _ := new(big.Int).SetString(s, 10)
		return i
	}

	type encodeTest struct {
		in  resp.Marshaler
		exp string

		// unmarshal is the string to unmarshal. defaults to exp if not set.
		unmarshal string

		// if set then when exp is unmarshaled back into in the value will be
		// asserted to be this value rather than in.
		expUnmarshal interface{}

		errStr bool

		// if set then the test won't be performed a second time with a
		// preceding attribute element when unmarshaling
		noAttrTest bool
	}

	encodeTests := []encodeTest{
		{in: &BlobStringBytes{B: nil}, exp: "$0\r\n\r\n"},
		{in: &BlobStringBytes{B: []byte{}}, exp: "$0\r\n\r\n",
			expUnmarshal: &BlobStringBytes{B: nil}},
		{in: &BlobStringBytes{B: []byte("foo")}, exp: "$3\r\nfoo\r\n"},
		{in: &BlobStringBytes{B: []byte("foo\r\nbar")}, exp: "$8\r\nfoo\r\nbar\r\n"},
		{in: &BlobStringBytes{StreamedStringHeader: true}, exp: "$?\r\n"},
		{in: &BlobString{S: ""}, exp: "$0\r\n\r\n"},
		{in: &BlobString{S: "foo"}, exp: "$3\r\nfoo\r\n"},
		{in: &BlobString{S: "foo\r\nbar"}, exp: "$8\r\nfoo\r\nbar\r\n"},
		{in: &BlobString{StreamedStringHeader: true}, exp: "$?\r\n"},

		{in: &SimpleString{S: ""}, exp: "+\r\n"},
		{in: &SimpleString{S: "foo"}, exp: "+foo\r\n"},

		{in: &SimpleError{S: ""}, exp: "-\r\n", errStr: true},
		{in: &SimpleError{S: "foo"}, exp: "-foo\r\n", errStr: true},

		{in: &Number{N: 5}, exp: ":5\r\n"},
		{in: &Number{N: 0}, exp: ":0\r\n"},
		{in: &Number{N: -5}, exp: ":-5\r\n"},

		{in: &Null{}, exp: "_\r\n"},
		{in: &Null{}, exp: "_\r\n", unmarshal: "$-1\r\n"},
		{in: &Null{}, exp: "_\r\n", unmarshal: "*-1\r\n"},

		{in: &Double{F: 0}, exp: ",0\r\n"},
		{in: &Double{F: 1.5}, exp: ",1.5\r\n"},
		{in: &Double{F: -1.5}, exp: ",-1.5\r\n"},
		{in: &Double{F: math.Inf(1)}, exp: ",inf\r\n"},
		{in: &Double{F: math.Inf(-1)}, exp: ",-inf\r\n"},

		{in: &Boolean{B: false}, exp: "#f\r\n"},
		{in: &Boolean{B: true}, exp: "#t\r\n"},

		{in: &BlobError{B: []byte("")}, exp: "!0\r\n\r\n"},
		{in: &BlobError{B: []byte("foo")}, exp: "!3\r\nfoo\r\n"},
		{in: &BlobError{B: []byte("foo\r\nbar")}, exp: "!8\r\nfoo\r\nbar\r\n"},

		{in: &VerbatimStringBytes{B: nil, Format: []byte("txt")}, exp: "=4\r\ntxt:\r\n"},
		{in: &VerbatimStringBytes{B: []byte{}, Format: []byte("txt")}, exp: "=4\r\ntxt:\r\n",
			expUnmarshal: &VerbatimStringBytes{B: nil, Format: []byte("txt")}},
		{in: &VerbatimStringBytes{B: []byte("foo"), Format: []byte("txt")}, exp: "=7\r\ntxt:foo\r\n"},
		{in: &VerbatimStringBytes{B: []byte("foo\r\nbar"), Format: []byte("txt")}, exp: "=12\r\ntxt:foo\r\nbar\r\n"},
		{in: &VerbatimString{S: "", Format: "txt"}, exp: "=4\r\ntxt:\r\n"},
		{in: &VerbatimString{S: "foo", Format: "txt"}, exp: "=7\r\ntxt:foo\r\n"},
		{in: &VerbatimString{S: "foo\r\nbar", Format: "txt"}, exp: "=12\r\ntxt:foo\r\nbar\r\n"},

		{in: &BigNumber{I: newBigInt("3492890328409238509324850943850943825024385")}, exp: "(3492890328409238509324850943850943825024385\r\n"},
		{in: &BigNumber{I: newBigInt("0")}, exp: "(0\r\n"},
		{in: &BigNumber{I: newBigInt("-3492890328409238509324850943850943825024385")}, exp: "(-3492890328409238509324850943850943825024385\r\n"},

		// TODO what to do with BulkReader
		//{in: &BulkReader{LR: newLR("foo\r\nbar")}, exp: "$8\r\nfoo\r\nbar\r\n"},

		{in: &ArrayHeader{NumElems: 0}, exp: "*0\r\n"},
		{in: &ArrayHeader{NumElems: 5}, exp: "*5\r\n"},
		{in: &ArrayHeader{StreamedArrayHeader: true}, exp: "*?\r\n"},

		{in: &MapHeader{NumPairs: 0}, exp: "%0\r\n"},
		{in: &MapHeader{NumPairs: 5}, exp: "%5\r\n"},
		{in: &MapHeader{StreamedMapHeader: true}, exp: "%?\r\n"},

		{in: &SetHeader{NumElems: 0}, exp: "~0\r\n"},
		{in: &SetHeader{NumElems: 5}, exp: "~5\r\n"},
		{in: &SetHeader{StreamedSetHeader: true}, exp: "~?\r\n"},

		{in: &AttributeHeader{NumPairs: 0}, exp: "|0\r\n", noAttrTest: true},
		{in: &AttributeHeader{NumPairs: 5}, exp: "|5\r\n", noAttrTest: true},

		{in: &PushHeader{NumElems: 0}, exp: ">0\r\n"},
		{in: &PushHeader{NumElems: 5}, exp: ">5\r\n"},

		{in: &StreamedStringChunkBytes{B: nil}, exp: ";0\r\n"},
		{in: &StreamedStringChunkBytes{B: []byte{}}, exp: ";0\r\n",
			expUnmarshal: &StreamedStringChunkBytes{B: nil}},
		{in: &StreamedStringChunkBytes{B: []byte("foo")}, exp: ";3\r\nfoo\r\n"},
		{in: &StreamedStringChunkBytes{B: []byte("foo\r\nbar")}, exp: ";8\r\nfoo\r\nbar\r\n"},
		{in: &StreamedStringChunk{S: ""}, exp: ";0\r\n"},
		{in: &StreamedStringChunk{S: "foo"}, exp: ";3\r\nfoo\r\n"},
		{in: &StreamedStringChunk{S: "foo\r\nbar"}, exp: ";8\r\nfoo\r\nbar\r\n"},
	}

	for i, et := range encodeTests {
		typName := reflect.TypeOf(et.in).Elem().String()
		t.Run(fmt.Sprintf("noAttr/%s/%d", typName, i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := et.in.MarshalRESP(buf)
			assert.Nil(t, err)
			assert.Equal(t, et.exp, buf.String())

			if et.unmarshal != "" {
				buf.Reset()
				buf.WriteString(et.unmarshal)
			}

			br := bufio.NewReader(buf)
			umr := reflect.New(reflect.TypeOf(et.in).Elem())
			um := umr.Interface().(resp.Unmarshaler)

			err = um.UnmarshalRESP(br)
			assert.Nil(t, err)
			assert.Empty(t, buf.String())

			var exp interface{} = et.in
			var got interface{} = umr.Interface()
			if et.errStr {
				exp = exp.(error).Error()
				got = got.(error).Error()
			} else if et.expUnmarshal != nil {
				exp = et.expUnmarshal
			}
			assert.Equal(t, exp, got)
		})
	}

	// do the unmarshal half of the tests again, but this time with a preceding
	// attribute which should be ignored.
	for i, et := range encodeTests {
		if et.noAttrTest {
			continue
		}

		typName := reflect.TypeOf(et.in).Elem().String()
		t.Run(fmt.Sprintf("attr/%s/%d", typName, i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			buf.WriteString("|1\r\n+foo\r\n+bar\r\n")
			if et.unmarshal != "" {
				buf.WriteString(et.unmarshal)
			} else {
				buf.WriteString(et.exp)
			}

			br := bufio.NewReader(buf)
			umr := reflect.New(reflect.TypeOf(et.in).Elem())
			um := umr.Interface().(resp.Unmarshaler)

			err := um.UnmarshalRESP(br)
			assert.Nil(t, err)
			assert.Empty(t, buf.String())

			var exp interface{} = et.in
			var got interface{} = umr.Interface()
			if et.errStr {
				exp = exp.(error).Error()
				got = got.(error).Error()
			} else if et.expUnmarshal != nil {
				exp = et.expUnmarshal
			}
			assert.Equal(t, exp, got)
		})
	}
}

// structs used for tests
type TestStructInner struct {
	Foo int
	bar int
	Baz string `redis:"BAZ"`
	Buz string `redis:"-"`
	Boz *int64
}

func intPtr(i int) *int {
	return &i
}

type testStructA struct {
	TestStructInner
	Biz []byte
}

type testStructB struct {
	*TestStructInner
	Biz []byte
}

type testStructC struct {
	Biz *string
}

type textCPMarshaler []byte

func (cm textCPMarshaler) MarshalText() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

type binCPMarshaler []byte

func (cm binCPMarshaler) MarshalBinary() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

/*
func TestAnyMarshal(t *T) {
	type encodeTest struct {
		in               interface{}
		out              string
		forceStr, flat   bool
		expErr           bool
		expErrConnUsable bool
	}

	var encodeTests = []encodeTest{
		// Bulk strings
		{in: []byte("ohey"), out: "$4\r\nohey\r\n"},
		{in: "ohey", out: "$4\r\nohey\r\n"},
		{in: "", out: "$0\r\n\r\n"},
		{in: true, out: "$1\r\n1\r\n"},
		{in: false, out: "$1\r\n0\r\n"},
		{in: nil, out: "$-1\r\n"},
		{in: nil, forceStr: true, out: "$0\r\n\r\n"},
		{in: []byte(nil), out: "$-1\r\n"},
		{in: []byte(nil), forceStr: true, out: "$0\r\n\r\n"},
		{in: float32(5.5), out: "$3\r\n5.5\r\n"},
		{in: float64(5.5), out: "$3\r\n5.5\r\n"},
		{in: textCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		{in: binCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		{in: "ohey", flat: true, out: "$4\r\nohey\r\n"},

		// Number
		{in: 5, out: ":5\r\n"},
		{in: int64(5), out: ":5\r\n"},
		{in: uint64(5), out: ":5\r\n"},
		{in: int64(5), forceStr: true, out: "$1\r\n5\r\n"},
		{in: uint64(5), forceStr: true, out: "$1\r\n5\r\n"},

		// Error
		{in: errors.New(":("), out: "-:(\r\n"},
		{in: errors.New(":("), forceStr: true, out: "$2\r\n:(\r\n"},

		// Simple arrays
		{in: []string(nil), out: "*-1\r\n"},
		{in: []string(nil), flat: true, out: ""},
		{in: []string{}, out: "*0\r\n"},
		{in: []string{}, flat: true, out: ""},
		{in: []string{"a", "b"}, out: "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{in: []int{1, 2}, out: "*2\r\n:1\r\n:2\r\n"},
		{in: []int{1, 2}, flat: true, out: ":1\r\n:2\r\n"},
		{in: []int{1, 2}, forceStr: true, out: "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"},
		{in: []int{1, 2}, flat: true, forceStr: true, out: "$1\r\n1\r\n$1\r\n2\r\n"},

		// Complex arrays
		{in: []interface{}{}, out: "*0\r\n"},
		{in: []interface{}{"a", 1}, out: "*2\r\n$1\r\na\r\n:1\r\n"},
		{
			in:       []interface{}{"a", 1},
			forceStr: true,
			out:      "*2\r\n$1\r\na\r\n$1\r\n1\r\n",
		},
		{
			in:       []interface{}{"a", 1},
			forceStr: true,
			flat:     true,
			out:      "$1\r\na\r\n$1\r\n1\r\n",
		},
		{
			in:     []interface{}{func() {}},
			expErr: true,
		},
		{
			in:               []interface{}{func() {}},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},
		{
			in:     []interface{}{"a", func() {}},
			flat:   true,
			expErr: true,
		},

		// Embedded arrays
		{
			in:  []interface{}{[]string{"a", "b"}, []int{1, 2}},
			out: "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n:1\r\n:2\r\n",
		},
		{
			in:   []interface{}{[]string{"a", "b"}, []int{1, 2}},
			flat: true,
			out:  "$1\r\na\r\n$1\r\nb\r\n:1\r\n:2\r\n",
		},
		{
			in: []interface{}{
				[]interface{}{"a"},
				[]interface{}{"b", func() {}},
			},
			expErr: true,
		},
		{
			in: []interface{}{
				[]interface{}{"a"},
				[]interface{}{"b", func() {}},
			},
			flat:   true,
			expErr: true,
		},
		{
			in: []interface{}{
				[]interface{}{func() {}, "a"},
				[]interface{}{"b", func() {}},
			},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},

		// Maps
		{in: map[string]int(nil), out: "*-1\r\n"},
		{in: map[string]int(nil), flat: true, out: ""},
		{in: map[string]int{}, out: "*0\r\n"},
		{in: map[string]int{}, flat: true, out: ""},
		{in: map[string]int{"one": 1}, out: "*2\r\n$3\r\none\r\n:1\r\n"},
		{
			in:  map[string]interface{}{"one": []byte("1")},
			out: "*2\r\n$3\r\none\r\n$1\r\n1\r\n",
		},
		{
			in:  map[string]interface{}{"one": []string{"1", "2"}},
			out: "*2\r\n$3\r\none\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n",
		},
		{
			in:   map[string]interface{}{"one": []string{"1", "2"}},
			flat: true,
			out:  "$3\r\none\r\n$1\r\n1\r\n$1\r\n2\r\n",
		},
		{
			in:     map[string]interface{}{"one": func() {}},
			expErr: true,
		},
		{
			in:     map[string]interface{}{"one": func() {}},
			flat:   true,
			expErr: true,
		},
		{
			in:     map[complex128]interface{}{0: func() {}},
			expErr: true,
		},
		{
			in:               map[complex128]interface{}{0: func() {}},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},

		// Structs
		{
			in: testStructA{
				TestStructInner: TestStructInner{
					Foo: 1,
					bar: 2,
					Baz: "3",
					Buz: "4",
					Boz: intPtr(5),
				},
				Biz: []byte("10"),
			},
			out: "*8\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBoz\r\n" + ":5\r\n" +
				"$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in: testStructB{
				TestStructInner: &TestStructInner{
					Foo: 1,
					bar: 2,
					Baz: "3",
					Buz: "4",
					Boz: intPtr(5),
				},
				Biz: []byte("10"),
			},
			out: "*8\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBoz\r\n" + ":5\r\n" +
				"$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in:  testStructB{Biz: []byte("10")},
			out: "*2\r\n" + "$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in:  testStructC{},
			out: "*2\r\n" + "$3\r\nBiz\r\n" + "$0\r\n\r\n",
		},
	}

	for i, et := range encodeTests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			a := Any{
				I:                     et.in,
				MarshalBlobString:     et.forceStr,
				MarshalNoArrayHeaders: et.flat,
			}

			err := a.MarshalRESP(buf)
			var errConnUsable resp.ErrConnUsable
			if et.expErr && err == nil {
				t.Fatal("expected error")
			} else if et.expErr && et.expErrConnUsable != errors.As(err, &errConnUsable) {
				t.Fatalf("expected ErrConnUsable:%v, got: %v", et.expErrConnUsable, err)
			} else if !et.expErr {
				assert.Nil(t, err)
			}

			if !et.expErr {
				assert.Equal(t, et.out, buf.String(), "et: %#v", et)
			}
		})
	}
}
*/

type textCPUnmarshaler []byte

func (cu *textCPUnmarshaler) UnmarshalText(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type binCPUnmarshaler []byte

func (cu *binCPUnmarshaler) UnmarshalBinary(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type lowerCaseUnmarshaler string

func (lcu *lowerCaseUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	var bs BlobString
	if err := bs.UnmarshalRESP(br); err != nil {
		return err
	}
	*lcu = lowerCaseUnmarshaler(strings.ToLower(bs.S))
	return nil
}

type upperCaseUnmarshaler string

func (ucu *upperCaseUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	var bs BlobString
	if err := bs.UnmarshalRESP(br); err != nil {
		return err
	}
	*ucu = upperCaseUnmarshaler(strings.ToUpper(bs.S))
	return nil
}

type writer []byte

func (w *writer) Write(b []byte) (int, error) {
	*w = append(*w, b...)
	return len(b), nil
}

func TestAnyUnmarshal(t *T) {
	type io [2]interface{}

	type decodeTest struct {
		descr string
		ins   []string

		// all ins will be unmarshaled into a pointer to an empty interface, and
		// that interface will be asserted to be equal to this value.
		defaultOut interface{}

		// for each in+io combination the in field will be unmarshaled into a
		// pointer to the first element of the io, then the first and second
		// elements of the io will be asserted to be equal.
		mkIO func() []io

		// instead of testing ios and defaultOut, assert that unmarshal returns
		// this specific error.
		shouldErr error
	}

	strPtr := func(s string) *string { return &s }
	bytPtr := func(b []byte) *[]byte { return &b }
	intPtr := func(i int64) *int64 { return &i }
	fltPtr := func(f float64) *float64 { return &f }

	strIntoOuts := func(str string) []io {
		return []io{
			{"", str},
			{"otherstring", str},
			{(*string)(nil), strPtr(str)},
			{strPtr(""), strPtr(str)},
			{strPtr("otherstring"), strPtr(str)},
			{[]byte{}, []byte(str)},
			{[]byte(nil), []byte(str)},
			{[]byte("f"), []byte(str)},
			{[]byte("biglongstringblaaaaah"), []byte(str)},
			{(*[]byte)(nil), bytPtr([]byte(str))},
			{bytPtr(nil), bytPtr([]byte(str))},
			{bytPtr([]byte("f")), bytPtr([]byte(str))},
			{bytPtr([]byte("biglongstringblaaaaah")), bytPtr([]byte(str))},
			{textCPUnmarshaler{}, textCPUnmarshaler(str)},
			{binCPUnmarshaler{}, binCPUnmarshaler(str)},
			{writer{}, writer(str)},
		}
	}

	floatIntoOuts := func(f float64) []io {
		return []io{
			{float32(0), float32(f)},
			{float32(1), float32(f)},
			{float64(0), float64(f)},
			{float64(1), float64(f)},
			{(*float64)(nil), fltPtr(f)},
			{fltPtr(0), fltPtr(f)},
			{fltPtr(1), fltPtr(f)},
			{new(big.Float), new(big.Float).SetFloat64(f)},
			{false, f != 0},
		}
	}

	intIntoOuts := func(i int64) []io {
		ios := append(
			floatIntoOuts(float64(i)),
			io{int(0), int(i)},
			io{int8(0), int8(i)},
			io{int16(0), int16(i)},
			io{int32(0), int32(i)},
			io{int64(0), int64(i)},
			io{int(1), int(i)},
			io{int8(1), int8(i)},
			io{int16(1), int16(i)},
			io{int32(1), int32(i)},
			io{int64(1), int64(i)},
			io{(*int64)(nil), intPtr(i)},
			io{intPtr(0), intPtr(i)},
			io{intPtr(1), intPtr(i)},
			io{new(big.Int), new(big.Int).SetInt64(i)},
		)
		if i >= 0 {
			ios = append(ios,
				io{uint(0), uint(i)},
				io{uint8(0), uint8(i)},
				io{uint16(0), uint16(i)},
				io{uint32(0), uint32(i)},
				io{uint64(0), uint64(i)},
				io{uint(1), uint(i)},
				io{uint8(1), uint8(i)},
				io{uint16(1), uint16(i)},
				io{uint32(1), uint32(i)},
				io{uint64(1), uint64(i)},
			)
		}
		return ios
	}

	nullIntoOuts := func() []io {
		return []io{
			{[]byte(nil), []byte(nil)},
			{[]byte{}, []byte(nil)},
			{[]byte{1}, []byte(nil)},
			{[]string(nil), []string(nil)},
			{[]string{}, []string(nil)},
			{[]string{"ohey"}, []string(nil)},
			{map[string]string(nil), map[string]string(nil)},
			{map[string]string{}, map[string]string(nil)},
			{map[string]string{"a": "b"}, map[string]string(nil)},
			{(*int64)(nil), (*int64)(nil)},
			{intPtr(0), (*int64)(nil)},
			{intPtr(1), (*int64)(nil)},
		}
	}

	decodeTests := []decodeTest{
		{
			descr:      "empty blob string",
			ins:        []string{"$0\r\n\r\n"},
			defaultOut: []byte{},
			mkIO:       func() []io { return strIntoOuts("") },
		},
		{
			descr:      "blob string",
			ins:        []string{"$4\r\nohey\r\n"},
			defaultOut: []byte("ohey"),
			mkIO:       func() []io { return strIntoOuts("ohey") },
		},
		{
			descr:      "integer blob string",
			ins:        []string{"$2\r\n10\r\n"},
			defaultOut: []byte("10"),
			mkIO:       func() []io { return append(strIntoOuts("10"), intIntoOuts(10)...) },
		},
		{
			descr:      "float blob string",
			ins:        []string{"$4\r\n10.5\r\n"},
			defaultOut: []byte("10.5"),
			mkIO:       func() []io { return append(strIntoOuts("10.5"), floatIntoOuts(10.5)...) },
		},
		{
			descr:      "null blob string", // only for backwards compatibility
			ins:        []string{"$-1\r\n"},
			defaultOut: []byte(nil),
			mkIO:       func() []io { return nullIntoOuts() },
		},
		{
			descr:      "blob string with delim",
			ins:        []string{"$6\r\nab\r\ncd\r\n"},
			defaultOut: []byte("ab\r\ncd"),
			mkIO:       func() []io { return strIntoOuts("ab\r\ncd") },
		},
		{
			descr:      "empty simple string",
			ins:        []string{"+\r\n"},
			defaultOut: "",
			mkIO:       func() []io { return strIntoOuts("") },
		},
		{
			descr:      "simple string",
			ins:        []string{"+ohey\r\n"},
			defaultOut: "ohey",
			mkIO:       func() []io { return strIntoOuts("ohey") },
		},
		{
			descr:      "integer simple string",
			ins:        []string{"+10\r\n"},
			defaultOut: "10",
			mkIO:       func() []io { return append(strIntoOuts("10"), intIntoOuts(10)...) },
		},
		{
			descr:      "float simple string",
			ins:        []string{"+10.5\r\n"},
			defaultOut: "10.5",
			mkIO:       func() []io { return append(strIntoOuts("10.5"), floatIntoOuts(10.5)...) },
		},
		{
			descr:     "empty simple error",
			ins:       []string{"-\r\n"},
			shouldErr: resp.ErrConnUsable{Err: SimpleError{S: ""}},
		},
		{
			descr:     "simple error",
			ins:       []string{"-ohey\r\n"},
			shouldErr: resp.ErrConnUsable{Err: SimpleError{S: "ohey"}},
		},
		{
			descr:      "zero number",
			ins:        []string{":0\r\n"},
			defaultOut: int64(0),
			mkIO:       func() []io { return append(strIntoOuts("0"), intIntoOuts(0)...) },
		},
		{
			descr:      "positive number",
			ins:        []string{":10\r\n"},
			defaultOut: int64(10),
			mkIO:       func() []io { return append(strIntoOuts("10"), intIntoOuts(10)...) },
		},
		{
			descr:      "negative number",
			ins:        []string{":-10\r\n"},
			defaultOut: int64(-10),
			mkIO:       func() []io { return append(strIntoOuts("-10"), intIntoOuts(-10)...) },
		},
		{
			descr:      "null",
			ins:        []string{"_\r\n"},
			defaultOut: nil,
			mkIO:       func() []io { return nullIntoOuts() },
		},
		{
			descr:      "zero double",
			ins:        []string{",0\r\n"},
			defaultOut: float64(0),
			mkIO:       func() []io { return append(strIntoOuts("0"), floatIntoOuts(0)...) },
		},
		{
			descr:      "positive double",
			ins:        []string{",10.5\r\n"},
			defaultOut: float64(10),
			mkIO:       func() []io { return append(strIntoOuts("10.5"), floatIntoOuts(10.5)...) },
		},
		{
			descr:      "positive double infinity",
			ins:        []string{",inf\r\n"},
			defaultOut: math.Inf(1),
			mkIO:       func() []io { return append(strIntoOuts("inf"), floatIntoOuts(math.Inf(1))...) },
		},
		{
			descr:      "negative double",
			ins:        []string{",-10.5\r\n"},
			defaultOut: float64(-10),
			mkIO:       func() []io { return append(strIntoOuts("-10.5"), floatIntoOuts(-10.5)...) },
		},
		{
			descr:      "negative double infinity",
			ins:        []string{",-inf\r\n"},
			defaultOut: math.Inf(-1),
			mkIO:       func() []io { return append(strIntoOuts("-inf"), floatIntoOuts(math.Inf(-1))...) },
		},
		{
			descr:      "true",
			ins:        []string{"#t\r\n"},
			defaultOut: true,
			// intIntoOuts will include actually unmarshaling into a bool
			mkIO: func() []io { return append(strIntoOuts("1"), intIntoOuts(1)...) },
		},
		{
			descr:      "false",
			ins:        []string{"#f\r\n"},
			defaultOut: false,
			// intIntoOuts will include actually unmarshaling into a bool
			mkIO: func() []io { return append(strIntoOuts("0"), intIntoOuts(0)...) },
		},
		{
			descr:     "empty blob error",
			ins:       []string{"!0\r\n\r\n"},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte{}}},
		},
		{
			descr:     "blob error",
			ins:       []string{"!4\r\nohey\r\n"},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte("ohey")}},
		},
		{
			descr:     "blob error with delim",
			ins:       []string{"!6\r\noh\r\ney\r\n"},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte("oh\r\ney")}},
		},
		{
			descr:      "empty verbatim string",
			ins:        []string{"=4\r\ntxt:\r\n"},
			defaultOut: "",
			mkIO:       func() []io { return strIntoOuts("") },
		},
		{
			descr:      "verbatim string",
			ins:        []string{"=8\r\ntxt:ohey\r\n"},
			defaultOut: "",
			mkIO:       func() []io { return strIntoOuts("ohey") },
		},
		{
			descr:      "verbatim string with delim",
			ins:        []string{"=10\r\ntxt:oh\r\ney\r\n"},
			defaultOut: "",
			mkIO:       func() []io { return strIntoOuts("oh\r\ney") },
		},
		{
			descr:      "zero big number",
			ins:        []string{"(0\r\n"},
			defaultOut: new(big.Int),
			mkIO:       func() []io { return append(strIntoOuts("0"), intIntoOuts(0)...) },
		},
		{
			descr:      "positive big number",
			ins:        []string{"(1000\r\n"},
			defaultOut: new(big.Int).SetInt64(1000),
			mkIO:       func() []io { return append(strIntoOuts("1000"), intIntoOuts(1000)...) },
		},
		{
			descr:      "negative big number",
			ins:        []string{"(-1000\r\n"},
			defaultOut: new(big.Int).SetInt64(-1000),
			mkIO:       func() []io { return append(strIntoOuts("-1000"), intIntoOuts(-1000)...) },
		},
		{
			descr:      "null array", // only for backwards compatibility
			ins:        []string{"*-1\r\n"},
			defaultOut: []interface{}(nil),
			mkIO:       func() []io { return nullIntoOuts() },
		},
		{
			descr: "empty agg",
			ins: []string{
				"*0\r\n",
				"%0\r\n",
				"~0\r\n",
				// push cannot be empty, don't test it here

				// equivalent streamed aggs
				"*?\r\n.\r\n",
				"%?\r\n.\r\n",
				"~?\r\n.\r\n",
			},
			defaultOut: []interface{}{},
			mkIO: func() []io {
				return []io{
					{[][]byte(nil), [][]byte{}},
					{[][]byte{}, [][]byte{}},
					{[][]byte{[]byte("a")}, [][]byte{}},
					{[]string(nil), []string{}},
					{[]string{}, []string{}},
					{[]string{"a"}, []string{}},
					{[]int(nil), []int{}},
					{[]int{}, []int{}},
					{[]int{5}, []int{}},
					{map[string][]byte(nil), map[string][]byte{}},
					{map[string][]byte{}, map[string][]byte{}},
					{map[string][]byte{"a": []byte("b")}, map[string][]byte{}},
					{map[string]string(nil), map[string]string{}},
					{map[string]string{}, map[string]string{}},
					{map[string]string{"a": "b"}, map[string]string{}},
					{map[int]int(nil), map[int]int{}},
					{map[int]int{}, map[int]int{}},
					{map[int]int{5: 5}, map[int]int{}},
				}
			},
		},
		{
			descr: "two element agg",
			ins: []string{
				"*2\r\n+666\r\n:1\r\n",
				"%1\r\n+666\r\n:1\r\n",
				"~2\r\n+666\r\n:1\r\n",
				">2\r\n+666\r\n:1\r\n",

				// equivalent streamed aggs
				"*?\r\n+666\r\n:1\r\n.\r\n",
				"%?\r\n+666\r\n:1\r\n.\r\n",
				"~?\r\n+666\r\n:1\r\n.\r\n",
			},
			defaultOut: []interface{}{"666", 1},
			mkIO: func() []io {
				return []io{
					{[][]byte(nil), [][]byte{[]byte("666"), []byte("1")}},
					{[][]byte{}, [][]byte{[]byte("666"), []byte("1")}},
					{[][]byte{[]byte("a")}, [][]byte{[]byte("666"), []byte("1")}},
					{[]string(nil), []string{"666", "1"}},
					{[]string{}, []string{"666", "1"}},
					{[]string{"a"}, []string{"666", "1"}},
					{[]int(nil), []int{666, 1}},
					{[]int{}, []int{666, 1}},
					{[]int{5}, []int{666, 1}},
					{map[string][]byte(nil), map[string][]byte{"666": []byte("1")}},
					{map[string][]byte{}, map[string][]byte{"666": []byte("1")}},
					{map[string][]byte{"a": []byte("b")}, map[string][]byte{"666": []byte("1")}},
					{map[string]string(nil), map[string]string{"666": "1"}},
					{map[string]string{}, map[string]string{"666": "1"}},
					{map[string]string{"a": "b"}, map[string]string{"666": "1"}},
					{map[int]int(nil), map[int]int{666: 1}},
					{map[int]int{}, map[int]int{666: 1}},
					{map[int]int{5: 5}, map[int]int{666: 1}},
				}
			},
		},
		{
			descr: "nested two element agg",
			ins: []string{
				"*1\r\n*2\r\n+666\r\n:1\r\n",
				"*1\r\n%1\r\n+666\r\n:1\r\n",
				"~1\r\n~2\r\n+666\r\n:1\r\n",
				"*1\r\n>2\r\n+666\r\n:1\r\n", // this is not possible but w/e

				// equivalent streamed aggs
				"*?\r\n*2\r\n+666\r\n:1\r\n.\r\n",
				"*?\r\n%1\r\n+666\r\n:1\r\n.\r\n",
				"~?\r\n~2\r\n+666\r\n:1\r\n.\r\n",
			},
			defaultOut: []interface{}{[]interface{}{"666", 1}},
			mkIO: func() []io {
				return []io{
					{[][][]byte(nil), [][][]byte{{[]byte("666"), []byte("1")}}},
					{[][][]byte{}, [][][]byte{{[]byte("666"), []byte("1")}}},
					{[][][]byte{{}, {[]byte("a")}}, [][][]byte{{[]byte("666"), []byte("1")}}},
					{[][]string(nil), [][]string{{"666", "1"}}},
					{[][]string{}, [][]string{{"666", "1"}}},
					{[][]string{{}, {"a"}}, [][]string{{"666", "1"}}},
					{[][]int(nil), [][]int{{666, 1}}},
					{[][]int{}, [][]int{{666, 1}}},
					{[][]int{{7}, {5}}, [][]int{{666, 1}}},
					{[]map[string][]byte(nil), []map[string][]byte{{"666": []byte("1")}}},
					{[]map[string][]byte{}, []map[string][]byte{{"666": []byte("1")}}},
					{[]map[string][]byte{{}, {"a": []byte("b")}}, []map[string][]byte{{"666": []byte("1")}}},
					{[]map[string]string(nil), []map[string]string{{"666": "1"}}},
					{[]map[string]string{}, []map[string]string{{"666": "1"}}},
					{[]map[string]string{{}, {"a": "b"}}, []map[string]string{{"666": "1"}}},
					{[]map[int]int(nil), []map[int]int{{666: 1}}},
					{[]map[int]int{}, []map[int]int{{666: 1}}},
					{[]map[int]int{{4: 2}, {7: 5}}, []map[int]int{{666: 1}}},
				}
			},
		},
		{
			descr: "keyed nested two element agg",
			ins: []string{
				"*2\r\n$2\r\n10\r\n*2\r\n+666\r\n:1\r\n",
				"%1\r\n$2\r\n10\r\n%1\r\n+666\r\n:1\r\n",
				"~2\r\n$2\r\n10\r\n~2\r\n+666\r\n:1\r\n",
				">2\r\n$2\r\n10\r\n>2\r\n+666\r\n:1\r\n",

				// equivalent streamed aggs
				"*?\r\n$2\r\n10\r\n*2\r\n+666\r\n:1\r\n.\r\n",
				"%?\r\n$2\r\n10\r\n%1\r\n+666\r\n:1\r\n.\r\n",
				"~?\r\n$2\r\n10\r\n~2\r\n+666\r\n:1\r\n.\r\n",
				">?\r\n$2\r\n10\r\n>2\r\n+666\r\n:1\r\n.\r\n",
			},
			defaultOut: []interface{}{[]byte("10"), []interface{}{"666", 1}},
			mkIO: func() []io {
				return []io{
					{map[string]map[string][]byte(nil), map[string]map[string][]byte{"10": {"666": []byte("1")}}},
					{map[string]map[string][]byte{}, map[string]map[string][]byte{"10": {"666": []byte("1")}}},
					{map[string]map[string][]byte{"foo": {"a": []byte("b")}}, map[string]map[string][]byte{"10": {"666": []byte("1")}}},
					{map[string]map[string]string(nil), map[string]map[string]string{"10": {"666": "1"}}},
					{map[string]map[string]string{}, map[string]map[string]string{"10": {"666": "1"}}},
					{map[string]map[string]string{"foo": {"a": "b"}}, map[string]map[string]string{"10": {"666": "1"}}},
					{map[string]map[int]int(nil), map[string]map[int]int{"10": {666: 1}}},
					{map[string]map[int]int{}, map[string]map[int]int{"10": {666: 1}}},
					{map[string]map[int]int{"foo": {4: 2}}, map[string]map[int]int{"10": {666: 1}}},
					{map[int]map[int]int(nil), map[int]map[int]int{10: {666: 1}}},
					{map[int]map[int]int{}, map[int]map[int]int{10: {666: 1}}},
					{map[int]map[int]int{5: {4: 2}}, map[int]map[int]int{10: {666: 1}}},
				}
			},
		},
		{
			descr: "agg into structs",
			ins: []string{
				"*10\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",
				"%5\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",
				"~10\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",

				// equivalent streamed aggs
				"*?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
				"%?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
				"~?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
			},
			defaultOut: []interface{}{"Foo", 1, "BAZ", 2, "Boz", 3, "Biz", 4},
			mkIO: func() []io {
				return []io{
					{testStructA{}, testStructA{TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}},
					{&testStructA{}, &testStructA{TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}},
					{testStructA{TestStructInner{bar: 6}, []byte("foo")}, testStructA{TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}},
					{&testStructA{TestStructInner{bar: 6}, []byte("foo")}, &testStructA{TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}},
					{testStructB{}, testStructB{&TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}},
					{&testStructB{}, &testStructB{&TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}},
					{testStructB{&TestStructInner{bar: 6}, []byte("foo")}, testStructB{&TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}},
					{&testStructB{&TestStructInner{bar: 6}, []byte("foo")}, &testStructB{&TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}},
				}
			},
		},
		{
			descr:      "empty streamed string",
			ins:        []string{"$?\r\n;0\r\n"},
			defaultOut: writer{},
			mkIO:       func() []io { return strIntoOuts("") },
		},
		{
			descr: "streamed string",
			ins: []string{
				"$?\r\n;4\r\nohey\r\n;0\r\n",
				"$?\r\n;2\r\noh\r\n;2\r\ney\r\n;0\r\n",
				"$?\r\n;1\r\no\r\n;1\r\nh\r\n;2\r\ney\r\n;0\r\n",
			},
			defaultOut: writer("ohey"),
			mkIO:       func() []io { return strIntoOuts("ohey") },
		},
	}

	for _, dt := range decodeTests {
		t.Run(dt.descr, func(t *testing.T) {
			for i, in := range dt.ins {
				if dt.shouldErr != nil {
					buf := bytes.NewBufferString(in)
					err := Any{}.UnmarshalRESP(bufio.NewReader(buf))
					assert.Equal(t, dt.shouldErr, err)
					assert.Empty(t, buf.Bytes())
					continue
				}

				t.Run("discard", func(t *testing.T) {
					buf := bytes.NewBufferString(in)
					br := bufio.NewReader(buf)
					err := Any{}.UnmarshalRESP(br)
					assert.NoError(t, err)
					assert.Empty(t, buf.Bytes())
				})

				t.Run(fmt.Sprintf("in%d", i), func(t *testing.T) {
					run := func(withAttr bool) func(t *testing.T) {
						return func(t *testing.T) {
							for j, io := range dt.mkIO() {
								t.Run(fmt.Sprintf("io%d", j), func(t *testing.T) {
									t.Logf("%q -> %#v", in, io[0])
									buf := bytes.NewBufferString(in)
									br := bufio.NewReader(buf)

									if withAttr {
										AttributeHeader{NumPairs: 2}.MarshalRESP(buf)
										SimpleString{S: "foo"}.MarshalRESP(buf)
										SimpleString{S: "1"}.MarshalRESP(buf)
										SimpleString{S: "bar"}.MarshalRESP(buf)
										SimpleString{S: "2"}.MarshalRESP(buf)
									}

									io0Val := reflect.ValueOf(io[0])
									into := reflect.New(io0Val.Type())
									into.Elem().Set(io0Val)

									err := Any{I: into.Interface()}.UnmarshalRESP(br)
									assert.NoError(t, err)

									io0 := into.Elem().Interface()
									switch io1 := io[1].(type) {
									case *big.Int:
										assert.Zero(t, io1.Cmp(io0.(*big.Int)))
									case *big.Float:
										assert.Zero(t, io1.Cmp(io0.(*big.Float)))
									default:
										assert.Equal(t, io[1], into.Elem().Interface())
									}
									assert.Empty(t, buf.Bytes())
								})
							}
						}
					}

					t.Run("with attr", run(true))
					t.Run("without attr", run(false))
				})
			}
		})
	}
}

func TestRawMessage(t *T) {
	rmtests := []struct {
		b       string
		isNil   bool
		isEmpty bool
	}{
		{b: "+\r\n"},
		{b: "+foo\r\n"},
		{b: "-\r\n"},
		{b: "-foo\r\n"},
		{b: ":5\r\n"},
		{b: ":0\r\n"},
		{b: ":-5\r\n"},
		{b: "$-1\r\n", isNil: true},
		{b: "$0\r\n\r\n"},
		{b: "$3\r\nfoo\r\n"},
		{b: "$8\r\nfoo\r\nbar\r\n"},
		{b: "*2\r\n:1\r\n:2\r\n"},
		{b: "*-1\r\n", isNil: true},
		{b: "*0\r\n", isEmpty: true},
	}

	// one at a time
	for _, rmt := range rmtests {
		buf := new(bytes.Buffer)
		{
			rm := RawMessage(rmt.b)
			require.Nil(t, rm.MarshalRESP(buf))
			assert.Equal(t, rmt.b, buf.String())
			assert.Equal(t, rmt.isNil, rm.IsNil())
			assert.Equal(t, rmt.isEmpty, rm.IsEmptyArray())
		}
		{
			var rm RawMessage
			require.Nil(t, rm.UnmarshalRESP(bufio.NewReader(buf)))
			assert.Equal(t, rmt.b, string(rm))
		}
	}
}

func TestAnyConsumedOnErr(t *T) {
	type foo struct {
		Foo int
		Bar int
	}

	type test struct {
		in   resp.Marshaler
		into interface{}
	}

	type unknownType string

	tests := []test{
		{Any{I: errors.New("foo")}, new(unknownType)},
		{BlobString{S: "blobStr"}, new(unknownType)},
		{SimpleString{S: "blobStr"}, new(unknownType)},
		{Number{N: 1}, new(unknownType)},
		{Any{I: []string{"one", "2", "three"}}, new([]int)},
		{Any{I: []string{"1", "2", "three", "four"}}, new([]int)},
		{Any{I: []string{"1", "2", "3", "four"}}, new([]int)},
		{Any{I: []string{"1", "2", "three", "four", "five"}}, new(map[int]int)},
		{Any{I: []string{"1", "2", "three", "four", "five", "six"}}, new(map[int]int)},
		{Any{I: []string{"1", "2", "3", "four", "five", "six"}}, new(map[int]int)},
		{Any{I: []interface{}{1, 2, "Bar", "two"}}, new(foo)},
		{Any{I: []string{"Foo", "1", "Bar", "two"}}, new(foo)},
		{Any{I: [][]string{{"one", "two"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "two"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "2"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "2"}, {"3", "four"}}}, new([][]int)},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			require.Nil(t, test.in.MarshalRESP(buf))
			require.Nil(t, SimpleString{S: "DISCARDED"}.MarshalRESP(buf))
			br := bufio.NewReader(buf)

			err := Any{I: test.into}.UnmarshalRESP(br)
			assert.Error(t, err)
			assert.True(t, errors.As(err, new(resp.ErrConnUsable)))

			var ss SimpleString
			assert.NoError(t, ss.UnmarshalRESP(br))
			assert.Equal(t, "DISCARDED", ss.S)
		})
	}
}

func Example_streamedAggregatedType() {
	buf := new(bytes.Buffer)

	// First write a streamed array to the buffer. The array will have 3 number
	// elements.
	(ArrayHeader{StreamedArrayHeader: true}).MarshalRESP(buf)
	(Number{N: 1}).MarshalRESP(buf)
	(Number{N: 2}).MarshalRESP(buf)
	(Number{N: 3}).MarshalRESP(buf)
	(StreamedAggregatedEnd{}).MarshalRESP(buf)

	// Now create a reader which will read from the buffer, and use it to read
	// the streamed array.
	br := bufio.NewReader(buf)
	var head ArrayHeader
	head.UnmarshalRESP(br)
	if !head.StreamedArrayHeader {
		panic("expected streamed array header")
	}
	fmt.Println("streamed array begun")

	for {
		var el Number
		aggEl := StreamedAggregatedElement{Unmarshaler: &el}
		aggEl.UnmarshalRESP(br)
		if aggEl.End {
			fmt.Println("streamed array ended")
			return
		}
		fmt.Printf("read element with value %d\n", el.N)
	}

	// Output: streamed array begun
	// Output: read element with value 1
	// Output: read element with value 2
	// Output: read element with value 3
	// Output: streamed array ended
}
