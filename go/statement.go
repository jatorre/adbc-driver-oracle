package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type statementImpl struct {
	driverbase.StatementImplBase
	cnxn  *connectionImpl
	alloc memory.Allocator
	query string
}

func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

func (s *statementImpl) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	rows, err := s.cnxn.db.QueryContext(ctx, s.query)
	if err != nil {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusIO, "query execution failed: %s", err)
	}

	impl := &oracleRecordReader{
		rows: rows,
	}

	var rr driverbase.BaseRecordReader
	if err := rr.Init(ctx, s.alloc, nil, driverbase.BaseRecordReaderOptions{}, impl); err != nil {
		rows.Close()
		return nil, -1, err
	}
	return &rr, -1, nil
}

func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if s.query == "" {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	result, err := s.cnxn.db.ExecContext(ctx, s.query)
	if err != nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusIO, "execute failed: %s", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusIO, "failed to get rows affected: %s", err)
	}
	return affected, nil
}

func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteSchema")
}

func (s *statementImpl) Prepare(ctx context.Context) error {
	// Oracle doesn't require explicit prepare — queries are prepared implicitly.
	// Just validate we have a query set.
	if s.query == "" {
		return s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}
	return nil
}

func (s *statementImpl) SetSubstraitPlan(plan []byte) error {
	return s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan")
}

func (s *statementImpl) Bind(ctx context.Context, values arrow.RecordBatch) error {
	return s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Bind")
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	return s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "BindStream")
}

func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetParameterSchema")
}

func (s *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecutePartitions")
}

func (s *statementImpl) Close() error {
	return nil
}

// oracleRecordReader implements driverbase.RecordReaderImpl.
// It handles both regular columns and SDO_GEOMETRY columns (via UDT registration).
type oracleRecordReader struct {
	rows     *sql.Rows
	colTypes []*sql.ColumnType
	scanDest []interface{}
	scanVals []interface{}
	geomIndices []int
	// firstRow holds a peeked first row (used to detect SRID before schema creation)
	firstRow     []interface{}
	hasFirstRow  bool
}

func (r *oracleRecordReader) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	colTypes, err := r.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	r.colTypes = colTypes

	fields := make([]arrow.Field, len(colTypes))
	r.geomIndices = nil

	// Detect geometry columns
	for i, ct := range colTypes {
		if strings.ToUpper(ct.DatabaseTypeName()) == "XMLTYPE" {
			r.geomIndices = append(r.geomIndices, i)
		}
	}

	// If there are geometry columns, peek at the first row to get the SRID
	// from the SdoGeometry struct itself — no extra query needed.
	var srid int64
	if len(r.geomIndices) > 0 {
		srid = r.peekSRID()
	}

	// Build schema
	for i, ct := range colTypes {
		nullable, _ := ct.Nullable()
		dbType := strings.ToUpper(ct.DatabaseTypeName())

		if dbType == "XMLTYPE" {
			fields[i] = GeoArrowWKBField(ct.Name(), srid, nullable)
		} else {
			fields[i] = arrow.Field{
				Name:     ct.Name(),
				Type:     oracleTypeToArrow(ct),
				Nullable: nullable,
			}
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func (r *oracleRecordReader) BeginAppending(builder *array.RecordBuilder) error {
	n := len(r.colTypes)
	r.scanDest = make([]interface{}, n)
	r.scanVals = make([]interface{}, n)
	for i := range r.scanDest {
		r.scanDest[i] = &r.scanVals[i]
	}
	return nil
}

func (r *oracleRecordReader) AppendRow(builder *array.RecordBuilder) (int64, error) {
	// Replay peeked first row if available
	if r.hasFirstRow {
		r.hasFirstRow = false
		copy(r.scanVals, r.firstRow)
		r.firstRow = nil
	} else {
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}

		if err := r.rows.Scan(r.scanDest...); err != nil {
			return 0, fmt.Errorf("scan error: %w", err)
		}
	}

	var rowSize int64
	geomSet := make(map[int]bool, len(r.geomIndices))
	for _, idx := range r.geomIndices {
		geomSet[idx] = true
	}

	for i, val := range r.scanVals {
		if geomSet[i] {
			size := r.appendGeometry(builder.Field(i), val)
			rowSize += size
		} else {
			size := appendValue(builder.Field(i), val, r.colTypes[i])
			rowSize += size
		}
	}

	return rowSize, nil
}

// appendGeometry converts an SdoGeometry value to WKB and appends to a BinaryBuilder.
func (r *oracleRecordReader) appendGeometry(fieldBuilder array.Builder, val interface{}) int64 {
	if val == nil {
		fieldBuilder.AppendNull()
		return 0
	}

	geom, ok := val.(SdoGeometry)
	if !ok {
		fieldBuilder.AppendNull()
		return 0
	}

	wkb, err := SdoToWKB(&geom)
	if err != nil {
		fieldBuilder.AppendNull()
		return 0
	}

	fieldBuilder.(*array.BinaryBuilder).Append(wkb)
	return int64(len(wkb))
}

// peekSRID reads the first row to extract the SRID from the first non-null geometry.
// The row is saved so AppendRow can replay it.
func (r *oracleRecordReader) peekSRID() int64 {
	if !r.rows.Next() {
		return 0
	}

	n := len(r.colTypes)
	dest := make([]interface{}, n)
	vals := make([]interface{}, n)
	for i := range dest {
		dest[i] = &vals[i]
	}

	if err := r.rows.Scan(dest...); err != nil {
		return 0
	}

	// Save the row so AppendRow replays it
	r.firstRow = vals
	r.hasFirstRow = true

	// Find SRID from first geometry column
	for _, idx := range r.geomIndices {
		if geom, ok := vals[idx].(SdoGeometry); ok && geom.SRID != 0 {
			return geom.SRID
		}
	}
	return 0
}

func (r *oracleRecordReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

func oracleTypeToArrow(ct *sql.ColumnType) arrow.DataType {
	dbType := strings.ToUpper(ct.DatabaseTypeName())

	switch {
	case dbType == "NUMBER" || dbType == "FLOAT" || dbType == "BINARY_FLOAT" || dbType == "BINARY_DOUBLE":
		precision, scale, ok := ct.DecimalSize()
		if ok && scale == 0 && precision > 0 && precision <= 18 {
			return arrow.PrimitiveTypes.Int64
		}
		return arrow.PrimitiveTypes.Float64

	case dbType == "VARCHAR2" || dbType == "VARCHAR" || dbType == "NVARCHAR2" ||
		dbType == "CHAR" || dbType == "NCHAR" || dbType == "CLOB" || dbType == "NCLOB" ||
		dbType == "LONG" || dbType == "ROWID":
		return arrow.BinaryTypes.String

	case dbType == "DATE" || dbType == "TIMESTAMP" || strings.HasPrefix(dbType, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us

	case dbType == "RAW" || dbType == "LONG RAW" || dbType == "BLOB":
		return arrow.BinaryTypes.Binary

	default:
		return arrow.BinaryTypes.String
	}
}
