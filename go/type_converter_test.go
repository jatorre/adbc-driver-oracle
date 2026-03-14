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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestAppendValue_Int64(t *testing.T) {
	alloc := memory.DefaultAllocator
	b := array.NewInt64Builder(alloc)
	defer b.Release()

	appendValue(b, int64(42), nil)
	appendValue(b, float64(99), nil)
	appendValue(b, "123", nil)
	appendValue(b, nil, nil)

	arr := b.NewInt64Array()
	defer arr.Release()

	if arr.Len() != 4 {
		t.Fatalf("expected 4 values, got %d", arr.Len())
	}
	if arr.Value(0) != 42 {
		t.Errorf("expected 42, got %d", arr.Value(0))
	}
	if arr.Value(1) != 99 {
		t.Errorf("expected 99, got %d", arr.Value(1))
	}
	if arr.Value(2) != 123 {
		t.Errorf("expected 123, got %d", arr.Value(2))
	}
	if !arr.IsNull(3) {
		t.Error("expected null at index 3")
	}
}

func TestAppendValue_Float64(t *testing.T) {
	alloc := memory.DefaultAllocator
	b := array.NewFloat64Builder(alloc)
	defer b.Release()

	appendValue(b, float64(3.14), nil)
	appendValue(b, int64(42), nil)
	appendValue(b, "2.718", nil)
	appendValue(b, nil, nil)

	arr := b.NewFloat64Array()
	defer arr.Release()

	if arr.Len() != 4 {
		t.Fatalf("expected 4 values, got %d", arr.Len())
	}
	if arr.Value(0) != 3.14 {
		t.Errorf("expected 3.14, got %f", arr.Value(0))
	}
	if arr.Value(1) != 42.0 {
		t.Errorf("expected 42.0, got %f", arr.Value(1))
	}
	if arr.Value(2) != 2.718 {
		t.Errorf("expected 2.718, got %f", arr.Value(2))
	}
	if !arr.IsNull(3) {
		t.Error("expected null at index 3")
	}
}

func TestAppendValue_String(t *testing.T) {
	alloc := memory.DefaultAllocator
	b := array.NewStringBuilder(alloc)
	defer b.Release()

	appendValue(b, "hello", nil)
	appendValue(b, []byte("bytes"), nil)
	appendValue(b, 42, nil)
	appendValue(b, nil, nil)

	arr := b.NewStringArray()
	defer arr.Release()

	if arr.Len() != 4 {
		t.Fatalf("expected 4 values, got %d", arr.Len())
	}
	if arr.Value(0) != "hello" {
		t.Errorf("expected 'hello', got %q", arr.Value(0))
	}
	if arr.Value(1) != "bytes" {
		t.Errorf("expected 'bytes', got %q", arr.Value(1))
	}
	if arr.Value(2) != "42" {
		t.Errorf("expected '42', got %q", arr.Value(2))
	}
	if !arr.IsNull(3) {
		t.Error("expected null at index 3")
	}
}

func TestGeoArrowWKBField(t *testing.T) {
	field := GeoArrowWKBField("geom", 4326, true)

	if field.Name != "geom" {
		t.Errorf("expected name 'geom', got %q", field.Name)
	}
	if field.Type != arrow.BinaryTypes.Binary {
		t.Errorf("expected Binary type, got %s", field.Type)
	}
	if !field.Nullable {
		t.Error("expected nullable")
	}

	extName, ok := field.Metadata.GetValue("ARROW:extension:name")
	if !ok || extName != "geoarrow.wkb" {
		t.Errorf("expected geoarrow.wkb, got %q", extName)
	}

	extMeta, ok := field.Metadata.GetValue("ARROW:extension:metadata")
	if !ok {
		t.Fatal("missing extension metadata")
	}
	if extMeta == "{}" {
		t.Error("SRID 4326 should produce non-empty metadata")
	}
	t.Logf("metadata: %s", extMeta)
}

func TestGeoArrowWKBField_NoSRID(t *testing.T) {
	field := GeoArrowWKBField("geom", 0, true)
	extMeta, _ := field.Metadata.GetValue("ARROW:extension:metadata")
	if extMeta != "{}" {
		t.Errorf("expected empty metadata for SRID=0, got %q", extMeta)
	}
}

func TestMatchLike(t *testing.T) {
	tests := []struct {
		value, pattern string
		want           bool
	}{
		{"ADMIN", "%", true},
		{"ADMIN", "ADMIN", true},
		{"ADMIN", "admin", true},
		{"ADMIN", "ADM%", true},
		{"ADMIN", "%MIN", true},
		{"ADMIN", "%DM%", true},
		{"ADMIN", "NOPE", false},
		{"ADMIN", "ADM", false},
		{"ADMIN", "%XYZ%", false},
		{"FIRES_WORLDWIDE", "FIRES%", true},
		{"FIRES_WORLDWIDE", "%WORLD%", true},
	}

	for _, tt := range tests {
		got := matchLike(tt.value, tt.pattern)
		if got != tt.want {
			t.Errorf("matchLike(%q, %q) = %v, want %v", tt.value, tt.pattern, got, tt.want)
		}
	}
}
