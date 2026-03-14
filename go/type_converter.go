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
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func appendValue(fieldBuilder array.Builder, val interface{}, ct *sql.ColumnType) int64 {
	if val == nil {
		fieldBuilder.AppendNull()
		return 0
	}

	switch b := fieldBuilder.(type) {
	case *array.Int64Builder:
		return appendInt64(b, val)
	case *array.Float64Builder:
		return appendFloat64(b, val)
	case *array.StringBuilder:
		return appendString(b, val)
	case *array.TimestampBuilder:
		return appendTimestamp(b, val)
	case *array.BinaryBuilder:
		return appendBinary(b, val)
	case *array.BooleanBuilder:
		return appendBoolean(b, val)
	default:
		// Fallback: convert to string
		s := fmt.Sprintf("%v", val)
		if sb, ok := fieldBuilder.(*array.StringBuilder); ok {
			sb.Append(s)
			return int64(len(s))
		}
		fieldBuilder.AppendNull()
		return 0
	}
}

func appendInt64(b *array.Int64Builder, val interface{}) int64 {
	switch v := val.(type) {
	case int64:
		b.Append(v)
	case int32:
		b.Append(int64(v))
	case int:
		b.Append(int64(v))
	case float64:
		b.Append(int64(v))
	case float32:
		b.Append(int64(v))
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			// Try parsing as float then truncate
			f, ferr := strconv.ParseFloat(v, 64)
			if ferr != nil {
				b.AppendNull()
			} else {
				b.Append(int64(f))
			}
		} else {
			b.Append(i)
		}
	default:
		b.AppendNull()
	}
	return 8
}

func appendFloat64(b *array.Float64Builder, val interface{}) int64 {
	switch v := val.(type) {
	case float64:
		b.Append(v)
	case float32:
		b.Append(float64(v))
	case int64:
		b.Append(float64(v))
	case int32:
		b.Append(float64(v))
	case int:
		b.Append(float64(v))
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			b.AppendNull()
		} else {
			b.Append(f)
		}
	default:
		b.AppendNull()
	}
	return 8
}

func appendString(b *array.StringBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case string:
		b.Append(v)
		return int64(len(v))
	case []byte:
		s := string(v)
		b.Append(s)
		return int64(len(s))
	default:
		s := fmt.Sprintf("%v", v)
		b.Append(s)
		return int64(len(s))
	}
}

func appendTimestamp(b *array.TimestampBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case time.Time:
		b.Append(arrow.Timestamp(v.UnixMicro()))
	case string:
		// Try common Oracle timestamp formats
		for _, layout := range []string{
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, v); err == nil {
				b.Append(arrow.Timestamp(t.UnixMicro()))
				return 8
			}
		}
		b.AppendNull()
	default:
		b.AppendNull()
	}
	return 8
}

func appendBinary(b *array.BinaryBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case []byte:
		b.Append(v)
		return int64(len(v))
	default:
		b.AppendNull()
		return 0
	}
}

func appendBoolean(b *array.BooleanBuilder, val interface{}) int64 {
	switch v := val.(type) {
	case bool:
		b.Append(v)
	case int64:
		b.Append(v != 0)
	case float64:
		b.Append(v != 0)
	case string:
		b.Append(v == "1" || v == "true" || v == "TRUE" || v == "Y")
	default:
		b.AppendNull()
	}
	return 1
}

