package oracle

import (
	"database/sql"
	"fmt"
	"math"
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
		b.AppendNull()
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
		b.Append(math.NaN())
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

