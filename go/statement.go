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

	// Detect SDO_GEOMETRY columns and rewrite query if needed
	rewrite, err := detectAndRewriteQuery(ctx, s.cnxn.db, s.query)
	if err != nil {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInternal, "query rewrite failed: %s", err)
	}

	rows, err := s.cnxn.db.QueryContext(ctx, rewrite.RewrittenQuery)
	if err != nil {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusIO, "query execution failed: %s", err)
	}

	impl := &oracleRecordReader{
		rows:        rows,
		rewrite:     rewrite,
		hasGeometry: len(rewrite.GeomColumns) > 0,
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
		return -1, nil
	}
	return affected, nil
}

func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteSchema")
}

func (s *statementImpl) Prepare(ctx context.Context) error {
	return s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Prepare")
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

// oracleRecordReader implements driverbase.RecordReaderImpl
type oracleRecordReader struct {
	rows        *sql.Rows
	colTypes    []*sql.ColumnType
	scanDest    []interface{}
	scanVals    []interface{}
	rewrite     *queryRewriteResult
	hasGeometry bool
	sridSeen    int64
	sridDetected bool
}

func (r *oracleRecordReader) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	if r.hasGeometry {
		return r.buildGeometrySchema()
	}
	schema, colTypes, err := buildSchemaFromRows(r.rows)
	if err != nil {
		return nil, err
	}
	r.colTypes = colTypes
	return schema, nil
}

// buildGeometrySchema builds the Arrow schema for a query with rewritten geometry columns.
// The rewritten query has expanded geometry columns into 7 fields each.
// The output schema collapses those back into single geoarrow.wkb binary columns.
func (r *oracleRecordReader) buildGeometrySchema() (*arrow.Schema, error) {
	colTypes, err := r.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	r.colTypes = colTypes

	// Build a mapping from rewritten column positions to output fields.
	// The rewritten query has extra columns for geometry decomposition.
	// We need to produce the original schema with geometry columns as geoarrow.wkb.
	var fields []arrow.Field

	rewrittenIdx := 0
	geomIdx := 0

	for _, origName := range r.rewrite.OriginalSchema {
		isGeom := false
		if geomIdx < len(r.rewrite.GeomColumns) && r.rewrite.GeomColumns[geomIdx].FieldIndex == rewrittenIdx {
			isGeom = true
		}

		if isGeom {
			fields = append(fields, GeoArrowWKBField(origName, 0, true))
			rewrittenIdx += geomFieldsPerColumn
			geomIdx++
		} else {
			ct := colTypes[rewrittenIdx]
			nullable, _ := ct.Nullable()
			fields = append(fields, arrow.Field{
				Name:     origName,
				Type:     oracleTypeToArrow(ct),
				Nullable: nullable,
			})
			rewrittenIdx++
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func (r *oracleRecordReader) BeginAppending(builder *array.RecordBuilder) error {
	// Allocate scan buffers for ALL columns in the rewritten query
	n := len(r.colTypes)
	r.scanDest = make([]interface{}, n)
	r.scanVals = make([]interface{}, n)
	for i := range r.scanDest {
		r.scanDest[i] = &r.scanVals[i]
	}
	return nil
}

func (r *oracleRecordReader) AppendRow(builder *array.RecordBuilder) (int64, error) {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}

	if err := r.rows.Scan(r.scanDest...); err != nil {
		return 0, fmt.Errorf("scan error: %w", err)
	}

	if r.hasGeometry {
		return r.appendRowWithGeometry(builder)
	}

	var rowSize int64
	for i, val := range r.scanVals {
		size := appendValue(builder.Field(i), val, r.colTypes[i])
		rowSize += size
	}
	return rowSize, nil
}

// appendRowWithGeometry handles rows where some columns are decomposed geometry fields.
func (r *oracleRecordReader) appendRowWithGeometry(builder *array.RecordBuilder) (int64, error) {
	var rowSize int64
	rewrittenIdx := 0
	outputIdx := 0
	geomIdx := 0

	for range r.rewrite.OriginalSchema {
		isGeom := false
		if geomIdx < len(r.rewrite.GeomColumns) && r.rewrite.GeomColumns[geomIdx].FieldIndex == rewrittenIdx {
			isGeom = true
		}

		if isGeom {
			wkb, size, err := reassembleGeometry(r.scanVals, rewrittenIdx)
			if err != nil {
				// On error, append null
				builder.Field(outputIdx).AppendNull()
			} else if wkb == nil {
				builder.Field(outputIdx).AppendNull()
			} else {
				builder.Field(outputIdx).(*array.BinaryBuilder).Append(wkb)
				rowSize += size
			}

			// Detect SRID from first non-null geometry
			if !r.sridDetected && r.scanVals[rewrittenIdx+1] != nil {
				r.sridSeen = int64(toFloat64(r.scanVals[rewrittenIdx+1]))
				r.sridDetected = true
			}

			rewrittenIdx += geomFieldsPerColumn
			geomIdx++
		} else {
			size := appendValue(builder.Field(outputIdx), r.scanVals[rewrittenIdx], r.colTypes[rewrittenIdx])
			rowSize += size
			rewrittenIdx++
		}
		outputIdx++
	}

	return rowSize, nil
}

func (r *oracleRecordReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

func buildSchemaFromRows(rows *sql.Rows) (*arrow.Schema, []*sql.ColumnType, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	fields := make([]arrow.Field, len(colTypes))
	for i, ct := range colTypes {
		nullable, _ := ct.Nullable()
		fields[i] = arrow.Field{
			Name:     ct.Name(),
			Type:     oracleTypeToArrow(ct),
			Nullable: nullable,
		}
	}

	return arrow.NewSchema(fields, nil), colTypes, nil
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

	case dbType == "MDSYS.SDO_GEOMETRY" || dbType == "SDO_GEOMETRY":
		return arrow.BinaryTypes.Binary

	default:
		return arrow.BinaryTypes.String
	}
}
