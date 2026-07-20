// Copyright 2025 CARTO
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oracle

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	go_ora "github.com/sijms/go-ora/v2"
)

func TestArrowToOracleType(t *testing.T) {
	cases := []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.PrimitiveTypes.Int64, "NUMBER(19)"},
		{arrow.PrimitiveTypes.Int8, "NUMBER(19)"},
		{arrow.PrimitiveTypes.Uint32, "NUMBER(19)"},
		{arrow.PrimitiveTypes.Uint64, "NUMBER(20)"},
		{arrow.PrimitiveTypes.Float32, "BINARY_FLOAT"},
		{arrow.PrimitiveTypes.Float64, "BINARY_DOUBLE"},
		{&arrow.Decimal128Type{Precision: 12, Scale: 3}, "NUMBER(12,3)"},
		// Precision beyond Oracle's 38 cap falls back to unconstrained NUMBER
		// (clamping would reject legitimate values with ORA-01438).
		{&arrow.Decimal256Type{Precision: 50, Scale: 4}, "NUMBER"},
		{&arrow.Decimal256Type{Precision: 38, Scale: 4}, "NUMBER(38,4)"},
		{arrow.BinaryTypes.String, "VARCHAR2(4000)"},
		{arrow.BinaryTypes.Binary, "BLOB"},
		{arrow.BinaryTypes.LargeBinary, "BLOB"},
		{&arrow.FixedSizeBinaryType{ByteWidth: 16}, "BLOB"},
		{arrow.FixedWidthTypes.Boolean, "NUMBER(1)"},
		{&arrow.TimestampType{Unit: arrow.Microsecond}, "TIMESTAMP(6)"},
		{&arrow.TimestampType{Unit: arrow.Millisecond}, "TIMESTAMP(6)"},
		{&arrow.TimestampType{Unit: arrow.Nanosecond}, "TIMESTAMP(9)"},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, "TIMESTAMP(6) WITH TIME ZONE"},
		{&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "America/New_York"}, "TIMESTAMP(9) WITH TIME ZONE"},
		{arrow.FixedWidthTypes.Date32, "DATE"},
		{arrow.FixedWidthTypes.Date64, "DATE"},
		{arrow.FixedWidthTypes.Time64us, "VARCHAR2(4000)"}, // fallback
	}
	for _, tc := range cases {
		if got := arrowToOracleType(tc.dt); got != tc.want {
			t.Errorf("arrowToOracleType(%s) = %q, want %q", tc.dt, got, tc.want)
		}
	}
}

func TestResolveColumnTypes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "narrow", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "wide", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "big", Type: arrow.BinaryTypes.LargeString, Nullable: true},
	}, nil)

	t.Run("auto uses widths", func(t *testing.T) {
		// With scan data available, LargeString is sized like String.
		widths := []int{0, 120, 28198, 5000}
		got := resolveColumnTypes(schema, widths, "", nil, maxInlineStringBytes)
		// VARCHAR2 is sized to the observed width (+headroom, 64-byte bucket),
		// not the inline limit — go-ora's read buffer scales with declared width.
		want := []string{"NUMBER(19)", "VARCHAR2(256)", "CLOB", "CLOB"}
		assertStrings(t, got, want)
	})

	t.Run("auto boundary at 4000 bytes", func(t *testing.T) {
		widths := []int{0, 4000, 4001, 10}
		got := resolveColumnTypes(schema, widths, "", nil, maxInlineStringBytes)
		// width 4000 fits inline (kept at 4000); 4001 > limit → CLOB; 10 → tight VARCHAR2(64)
		want := []string{"NUMBER(19)", "VARCHAR2(4000)", "CLOB", "VARCHAR2(64)"}
		assertStrings(t, got, want)
	})

	t.Run("no widths defaults to varchar2 except large_string", func(t *testing.T) {
		got := resolveColumnTypes(schema, nil, "", nil, maxInlineStringBytes)
		want := []string{"NUMBER(19)", "VARCHAR2(4000)", "VARCHAR2(4000)", "CLOB"}
		assertStrings(t, got, want)
	})

	t.Run("forced clob", func(t *testing.T) {
		got := resolveColumnTypes(schema, nil, "clob", nil, maxInlineStringBytes)
		want := []string{"NUMBER(19)", "CLOB", "CLOB", "CLOB"}
		assertStrings(t, got, want)
	})

	t.Run("forced varchar2", func(t *testing.T) {
		got := resolveColumnTypes(schema, nil, "varchar2", nil, maxInlineStringBytes)
		want := []string{"NUMBER(19)", "VARCHAR2(4000)", "VARCHAR2(4000)", "VARCHAR2(4000)"}
		assertStrings(t, got, want)
	})

	t.Run("clob_columns overrides scan", func(t *testing.T) {
		widths := []int{0, 10, 10, 10}
		got := resolveColumnTypes(schema, widths, "", map[string]bool{"NARROW": true}, maxInlineStringBytes)
		want := []string{"NUMBER(19)", "CLOB", "VARCHAR2(64)", "VARCHAR2(64)"}
		assertStrings(t, got, want)
	})
}

func TestBuildCreateTableDDL(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	ddl := buildCreateTableDDL("MY_TABLE", schema, []string{"NUMBER(19)", "CLOB"})
	want := `CREATE TABLE MY_TABLE ("ID" NUMBER(19) NOT NULL, "DATA" CLOB) NOLOGGING`
	if ddl != want {
		t.Errorf("ddl = %q, want %q", ddl, want)
	}

	// nil colTypes falls back to data-independent defaults
	ddl = buildCreateTableDDL("MY_TABLE", schema, nil)
	want = `CREATE TABLE MY_TABLE ("ID" NUMBER(19) NOT NULL, "DATA" VARCHAR2(4000)) NOLOGGING`
	if ddl != want {
		t.Errorf("ddl = %q, want %q", ddl, want)
	}
}

func TestScanStringWidths(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "ls", Type: arrow.BinaryTypes.LargeString, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"ab", strings.Repeat("é", 3000), ""}, []bool{true, true, false})
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(2).(*array.LargeStringBuilder).AppendValues([]string{"xyz", "", "longest-one"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	widths := make([]int, 3)
	scanStringWidths(rec, widths)
	// "é" is 2 bytes in UTF-8 — widths must be byte lengths, not rune counts.
	if widths[0] != 6000 {
		t.Errorf("widths[0] = %d, want 6000 (byte length)", widths[0])
	}
	if widths[1] != 0 {
		t.Errorf("widths[1] = %d, want 0 (non-string col)", widths[1])
	}
	if widths[2] != len("longest-one") {
		t.Errorf("widths[2] = %d, want %d", widths[2], len("longest-one"))
	}

	// A second scan must keep the running max.
	b2 := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b2.Release()
	b2.Field(0).(*array.StringBuilder).AppendValues([]string{"tiny"}, nil)
	b2.Field(1).(*array.Int64Builder).AppendValues([]int64{4}, nil)
	b2.Field(2).(*array.LargeStringBuilder).AppendValues([]string{"a"}, nil)
	rec2 := b2.NewRecord()
	defer rec2.Release()
	scanStringWidths(rec2, widths)
	if widths[0] != 6000 {
		t.Errorf("widths[0] after second scan = %d, want 6000", widths[0])
	}
}

func TestArrowColumnToSliceRangeTemporal(t *testing.T) {
	t.Run("naive timestamp binds as go_ora.NullTimeStamp", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Microsecond}
		b := array.NewTimestampBuilder(memory.DefaultAllocator, dt)
		defer b.Release()
		want := time.Date(2024, 3, 15, 10, 30, 45, 123456000, time.UTC)
		ts, _ := arrow.TimestampFromTime(want, arrow.Microsecond)
		b.Append(ts)
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()

		got, ok := arrowColumnToSliceRange(arr, 0, 2, maxInlineStringBytes).([]go_ora.NullTimeStamp)
		if !ok {
			t.Fatalf("expected []go_ora.NullTimeStamp, got %T", arrowColumnToSliceRange(arr, 0, 2, maxInlineStringBytes))
		}
		if !got[0].Valid || !time.Time(got[0].TimeStamp).Equal(want) {
			t.Errorf("got %v, want %v", time.Time(got[0].TimeStamp), want)
		}
		if got[1].Valid {
			t.Error("expected null at index 1")
		}
	})

	t.Run("tz-aware timestamp binds as sql.NullTime preserving instant", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"}
		b := array.NewTimestampBuilder(memory.DefaultAllocator, dt)
		defer b.Release()
		instant := time.Date(2024, 6, 1, 16, 0, 0, 500000000, time.UTC)
		ts, _ := arrow.TimestampFromTime(instant, arrow.Microsecond)
		b.Append(ts)
		arr := b.NewArray()
		defer arr.Release()

		got, ok := arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes).([]sql.NullTime)
		if !ok {
			t.Fatalf("expected []sql.NullTime, got %T", arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes))
		}
		if !got[0].Valid || !got[0].Time.Equal(instant) {
			t.Errorf("got %v, want instant %v", got[0].Time, instant)
		}
	})

	t.Run("millisecond unit converts correctly", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Millisecond}
		b := array.NewTimestampBuilder(memory.DefaultAllocator, dt)
		defer b.Release()
		want := time.Date(2024, 3, 15, 10, 30, 45, 123000000, time.UTC)
		ts, _ := arrow.TimestampFromTime(want, arrow.Millisecond)
		b.Append(ts)
		arr := b.NewArray()
		defer arr.Release()

		got := arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes).([]go_ora.NullTimeStamp)
		if !time.Time(got[0].TimeStamp).Equal(want) {
			t.Errorf("got %v, want %v (unit must not be assumed µs)", time.Time(got[0].TimeStamp), want)
		}
	})

	t.Run("date32 binds as midnight timestamp", func(t *testing.T) {
		b := array.NewDate32Builder(memory.DefaultAllocator)
		defer b.Release()
		d := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
		b.Append(arrow.Date32FromTime(d))
		arr := b.NewArray()
		defer arr.Release()

		got := arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes).([]go_ora.NullTimeStamp)
		if !time.Time(got[0].TimeStamp).Equal(d) {
			t.Errorf("got %v, want %v", time.Time(got[0].TimeStamp), d)
		}
	})
}

func TestArrowColumnToSliceRangeScalars(t *testing.T) {
	t.Run("bool binds as 0/1 ints", func(t *testing.T) {
		b := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues([]bool{true, false}, nil)
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()

		got, ok := arrowColumnToSliceRange(arr, 0, 3, maxInlineStringBytes).([]sql.NullInt64)
		if !ok {
			t.Fatalf("expected []sql.NullInt64, got %T", arrowColumnToSliceRange(arr, 0, 3, maxInlineStringBytes))
		}
		if got[0].Int64 != 1 || got[1].Int64 != 0 || got[2].Valid {
			t.Errorf("got %v, want [1 0 null]", got)
		}
	})

	t.Run("uint64 above int64 max binds as decimal text", func(t *testing.T) {
		b := array.NewUint64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues([]uint64{18446744073709551615, 42}, nil)
		arr := b.NewArray()
		defer arr.Release()

		got := arrowColumnToSliceRange(arr, 0, 2, maxInlineStringBytes).([]sql.NullString)
		if got[0].String != "18446744073709551615" || got[1].String != "42" {
			t.Errorf("got %v", got)
		}
	})

	t.Run("float32 binds as NullFloat64", func(t *testing.T) {
		b := array.NewFloat32Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues([]float32{1.5}, nil)
		arr := b.NewArray()
		defer arr.Release()

		got := arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes).([]sql.NullFloat64)
		if got[0].Float64 != 1.5 {
			t.Errorf("got %v", got)
		}
	})

	t.Run("small ints bind as NullInt64", func(t *testing.T) {
		b := array.NewInt16Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues([]int16{-7}, nil)
		arr := b.NewArray()
		defer arr.Release()

		got := arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes).([]sql.NullInt64)
		if got[0].Int64 != -7 {
			t.Errorf("got %v", got)
		}
	})

	t.Run("decimal256 binds as scaled text", func(t *testing.T) {
		dt := &arrow.Decimal256Type{Precision: 40, Scale: 2}
		b := array.NewDecimal256Builder(memory.DefaultAllocator, dt)
		defer b.Release()
		if err := b.AppendValueFromString("12345.67"); err != nil {
			t.Fatal(err)
		}
		arr := b.NewArray()
		defer arr.Release()

		got := arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes).([]sql.NullString)
		if got[0].String != "12345.67" {
			t.Errorf("got %q, want 12345.67", got[0].String)
		}
	})
}

// Batches with values beyond the VARCHAR2 cap must switch to the go_ora.Clob
// bind: plain string binds ride go-ora's negotiated varchar limit and can fail
// on MAX_STRING_SIZE=STANDARD servers even when the column is CLOB.
func TestStringColumnToSliceWideBindsAsClob(t *testing.T) {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	wide := strings.Repeat("x", 4001)
	b.AppendValues([]string{wide, "small"}, nil)
	b.AppendNull()
	arr := b.NewArray()
	defer arr.Release()

	got, ok := arrowColumnToSliceRange(arr, 0, 3, maxInlineStringBytes).([]go_ora.Clob)
	if !ok {
		t.Fatalf("expected []go_ora.Clob for wide batch, got %T", arrowColumnToSliceRange(arr, 0, 3, maxInlineStringBytes))
	}
	if !got[0].Valid || got[0].String != wide {
		t.Errorf("wide value mismatch: valid=%v len=%d", got[0].Valid, len(got[0].String))
	}
	if !got[1].Valid || got[1].String != "small" {
		t.Errorf("narrow value mismatch: %+v", got[1])
	}
	if got[2].Valid {
		t.Error("expected null at index 2")
	}
}

// On MAX_STRING_SIZE=EXTENDED servers a value between the STANDARD cap and
// 32767 bytes stays a plain VARCHAR2 bind (fast batch array), not a per-row
// go_ora.Clob — this is what keeps wide-text ingest (e.g. OSM other_tags) off
// the slow temporary-LOB path. The same value on a STANDARD server must fall
// back to CLOB. Both the bind type and the DDL type track inlineLimit together.
func TestStringColumnExtendedVarcharAvoidsClob(t *testing.T) {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	mid := strings.Repeat("x", 5000) // > 4000, < 32767
	b.AppendValues([]string{mid, "small"}, nil)
	arr := b.NewArray()
	defer arr.Release()

	if _, ok := arrowColumnToSliceRange(arr, 0, 2, maxExtendedStringBytes).([]sql.NullString); !ok {
		t.Fatalf("EXTENDED: expected []sql.NullString for 5000-byte value, got %T",
			arrowColumnToSliceRange(arr, 0, 2, maxExtendedStringBytes))
	}
	if _, ok := arrowColumnToSliceRange(arr, 0, 2, maxInlineStringBytes).([]go_ora.Clob); !ok {
		t.Fatalf("STANDARD: expected []go_ora.Clob for 5000-byte value, got %T",
			arrowColumnToSliceRange(arr, 0, 2, maxInlineStringBytes))
	}

	schema := arrow.NewSchema([]arrow.Field{{Name: "tags", Type: arrow.BinaryTypes.String}}, nil)
	widths := []int{5000}
	if got := resolveColumnTypes(schema, widths, "", nil, maxExtendedStringBytes); got[0] != "VARCHAR2(32767)" {
		t.Errorf("EXTENDED DDL: got %q, want VARCHAR2(32767)", got[0])
	}
	if got := resolveColumnTypes(schema, widths, "", nil, maxInlineStringBytes); got[0] != "CLOB" {
		t.Errorf("STANDARD DDL: got %q, want CLOB", got[0])
	}
}

// Strings and binaries must be deep-copied: the pipeline path converts a
// record in a goroutine, releases it, and only then does go-ora serialize the
// bind buffers. Aliased values would be use-after-free.
func TestArrowColumnToSliceRangeDeepCopies(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)

	t.Run("large string", func(t *testing.T) {
		b := array.NewLargeStringBuilder(alloc)
		b.AppendValues([]string{"hello world"}, nil)
		arr := b.NewArray()
		b.Release()

		got := arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes).([]sql.NullString)
		arr.Release() // buffers returned to the allocator here
		if got[0].String != "hello world" {
			t.Errorf("got %q", got[0].String)
		}
	})

	t.Run("large binary", func(t *testing.T) {
		b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.LargeBinary)
		b.Append([]byte{1, 2, 3})
		arr := b.NewArray()
		b.Release()

		got := arrowColumnToSliceRange(arr, 0, 1, maxInlineStringBytes).([][]byte)
		arr.Release()
		if len(got[0]) != 3 || got[0][0] != 1 {
			t.Errorf("got %v", got[0])
		}
	})

	alloc.AssertSize(t, 0)
}

func TestIsOraCode(t *testing.T) {
	if !isOraCode(errors.New("ORA-00955: name is already used by an existing object"), 955) {
		t.Error("expected match for ORA-00955")
	}
	if isOraCode(errors.New("ORA-00942: table or view does not exist"), 955) {
		t.Error("unexpected match")
	}
	if isOraCode(nil, 955) {
		t.Error("nil error must not match")
	}
}

func assertStrings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("index %d: got %q, want %q", i, got[i], want[i])
		}
	}
}
