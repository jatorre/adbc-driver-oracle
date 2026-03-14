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

	// Prepared statement support
	prepared *sql.Stmt

	// Bind parameters (single batch)
	bindParams arrow.RecordBatch
	// Bind stream (multiple batches for bulk ingest)
	bindStream array.RecordReader

	// Ingest options
	ingestTargetTable string
	ingestMode        string // "create", "append", "replace", "create_append"
}

func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

func (s *statementImpl) SetSqlQuery(query string) error {
	s.query = query
	s.closePrepared()
	return nil
}

func (s *statementImpl) SetOption(key, value string) error {
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		s.ingestTargetTable = value
		return nil
	case adbc.OptionKeyIngestMode:
		s.ingestMode = value
		return nil
	default:
		return s.StatementImplBase.SetOption(key, value)
	}
}

func (s *statementImpl) GetOption(key string) (string, error) {
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		return s.ingestTargetTable, nil
	case adbc.OptionKeyIngestMode:
		return s.ingestMode, nil
	default:
		return s.StatementImplBase.GetOption(key)
	}
}

func (s *statementImpl) Prepare(ctx context.Context) error {
	if s.query == "" {
		return s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}
	s.closePrepared()
	stmt, err := s.cnxn.db.PrepareContext(ctx, s.query)
	if err != nil {
		return s.ErrorHelper.Errorf(adbc.StatusIO, "prepare failed: %s", err)
	}
	s.prepared = stmt
	return nil
}

func (s *statementImpl) Bind(ctx context.Context, values arrow.RecordBatch) error {
	if values != nil {
		values.Retain()
	}
	if s.bindParams != nil {
		s.bindParams.Release()
	}
	s.bindParams = values
	return nil
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if s.bindStream != nil {
		s.bindStream.Release()
	}
	if stream != nil {
		stream.Retain()
	}
	s.bindStream = stream
	return nil
}

func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		return nil, -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	// Build query args from bind parameters
	args := s.extractBindArgs(0)

	var rows *sql.Rows
	var err error
	if s.prepared != nil {
		rows, err = s.prepared.QueryContext(ctx, args...)
	} else {
		rows, err = s.cnxn.db.QueryContext(ctx, s.query, args...)
	}
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
	// Bulk ingest path
	if s.ingestTargetTable != "" {
		return s.executeBulkIngest(ctx)
	}

	if s.query == "" {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no query set")
	}

	args := s.extractBindArgs(0)

	var result sql.Result
	var err error
	if s.prepared != nil {
		result, err = s.prepared.ExecContext(ctx, args...)
	} else {
		result, err = s.cnxn.db.ExecContext(ctx, s.query, args...)
	}
	if err != nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusIO, "execute failed: %s", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusIO, "failed to get rows affected: %s", err)
	}
	return affected, nil
}

// extractBindArgs extracts parameter values from the bind RecordBatch at the given row index.
// Oracle uses :1, :2, ... as bind placeholders.
func (s *statementImpl) extractBindArgs(rowIdx int) []interface{} {
	if s.bindParams == nil || s.bindParams.NumCols() == 0 {
		return nil
	}
	if int64(rowIdx) >= s.bindParams.NumRows() {
		return nil
	}

	args := make([]interface{}, s.bindParams.NumCols())
	for i := 0; i < int(s.bindParams.NumCols()); i++ {
		col := s.bindParams.Column(i)
		if col.IsNull(rowIdx) {
			args[i] = nil
			continue
		}
		args[i] = extractArrowValue(col, rowIdx)
	}
	return args
}

// extractArrowValue reads a single value from an Arrow array at the given index.
func extractArrowValue(arr arrow.Array, idx int) interface{} {
	switch a := arr.(type) {
	case *array.Int64:
		return a.Value(idx)
	case *array.Int32:
		return a.Value(idx)
	case *array.Float64:
		return a.Value(idx)
	case *array.Float32:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	case *array.Binary:
		return a.Value(idx)
	case *array.Boolean:
		return a.Value(idx)
	case *array.Timestamp:
		return a.Value(idx).ToTime(arrow.Microsecond)
	default:
		return fmt.Sprintf("%v", a)
	}
}

// --- Bulk Ingest ---

func (s *statementImpl) executeBulkIngest(ctx context.Context) (int64, error) {
	tableName := strings.ToUpper(s.ingestTargetTable)

	// Determine the schema from bindStream or bindParams
	var schema *arrow.Schema
	if s.bindStream != nil {
		schema = s.bindStream.Schema()
	} else if s.bindParams != nil {
		schema = s.bindParams.Schema()
	} else {
		return -1, s.ErrorHelper.Errorf(adbc.StatusInvalidState, "no data bound for ingest")
	}

	mode := s.ingestMode
	if mode == "" {
		mode = adbc.OptionValueIngestModeCreate
	}

	// Handle table creation/replacement based on mode
	if err := s.prepareIngestTable(ctx, tableName, schema, mode); err != nil {
		return -1, err
	}

	// Build INSERT statement: INSERT INTO table (col1, col2, ...) VALUES (:1, :2, ...)
	colNames := make([]string, schema.NumFields())
	placeholders := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		colNames[i] = fmt.Sprintf(`"%s"`, strings.ToUpper(f.Name))
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	stmt, err := s.cnxn.db.PrepareContext(ctx, insertSQL)
	if err != nil {
		return -1, s.ErrorHelper.Errorf(adbc.StatusIO, "prepare insert failed: %s", err)
	}
	defer stmt.Close()

	var totalRows int64

	if s.bindStream != nil {
		// Process all batches from the stream
		for s.bindStream.Next() {
			rec := s.bindStream.Record()
			n, err := s.insertBatch(ctx, stmt, rec)
			if err != nil {
				return totalRows, err
			}
			totalRows += n
		}
		if err := s.bindStream.Err(); err != nil {
			return totalRows, s.ErrorHelper.Errorf(adbc.StatusIO, "bind stream error: %s", err)
		}
	} else if s.bindParams != nil {
		n, err := s.insertBatch(ctx, stmt, s.bindParams)
		if err != nil {
			return 0, err
		}
		totalRows = n
	}

	return totalRows, nil
}

func (s *statementImpl) insertBatch(ctx context.Context, stmt *sql.Stmt, rec arrow.RecordBatch) (int64, error) {
	numRows := rec.NumRows()
	numCols := int(rec.NumCols())
	var inserted int64

	for row := 0; row < int(numRows); row++ {
		args := make([]interface{}, numCols)
		for col := 0; col < numCols; col++ {
			arr := rec.Column(col)
			if arr.IsNull(row) {
				args[col] = nil
			} else {
				args[col] = extractArrowValue(arr, row)
			}
		}

		_, err := stmt.ExecContext(ctx, args...)
		if err != nil {
			return inserted, s.ErrorHelper.Errorf(adbc.StatusIO, "insert row %d failed: %s", row, err)
		}
		inserted++
	}

	// Commit after each batch
	if _, err := s.cnxn.db.ExecContext(ctx, "COMMIT"); err != nil {
		return inserted, s.ErrorHelper.Errorf(adbc.StatusIO, "commit failed: %s", err)
	}

	return inserted, nil
}

func (s *statementImpl) prepareIngestTable(ctx context.Context, tableName string, schema *arrow.Schema, mode string) error {
	switch mode {
	case adbc.OptionValueIngestModeCreate:
		// Create table — fail if exists
		ddl := buildCreateTableDDL(tableName, schema)
		if _, err := s.cnxn.db.ExecContext(ctx, ddl); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusIO, "create table failed: %s", err)
		}
	case adbc.OptionValueIngestModeAppend:
		// Table must exist — do nothing
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate
		s.cnxn.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s PURGE", tableName))
		ddl := buildCreateTableDDL(tableName, schema)
		if _, err := s.cnxn.db.ExecContext(ctx, ddl); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusIO, "create table failed: %s", err)
		}
	case adbc.OptionValueIngestModeCreateAppend:
		// Create if not exists, append if exists
		ddl := buildCreateTableDDL(tableName, schema)
		s.cnxn.db.ExecContext(ctx, ddl) // ignore error (table may exist)
	default:
		return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "unknown ingest mode: %s", mode)
	}
	return nil
}

func buildCreateTableDDL(tableName string, schema *arrow.Schema) string {
	var cols []string
	for _, f := range schema.Fields() {
		oraType := arrowToOracleType(f.Type)
		nullable := ""
		if !f.Nullable {
			nullable = " NOT NULL"
		}
		cols = append(cols, fmt.Sprintf(`"%s" %s%s`, strings.ToUpper(f.Name), oraType, nullable))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(cols, ", "))
}

func arrowToOracleType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return "NUMBER(19)"
	case arrow.FLOAT16, arrow.FLOAT32:
		return "BINARY_FLOAT"
	case arrow.FLOAT64:
		return "BINARY_DOUBLE"
	case arrow.STRING, arrow.LARGE_STRING:
		return "VARCHAR2(4000)"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "BLOB"
	case arrow.BOOL:
		return "NUMBER(1)"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	default:
		return "VARCHAR2(4000)"
	}
}

// --- Unimplemented ---

func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteSchema")
}

func (s *statementImpl) SetSubstraitPlan(plan []byte) error {
	return s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan")
}

func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetParameterSchema")
}

func (s *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, s.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecutePartitions")
}

func (s *statementImpl) Close() error {
	s.closePrepared()
	if s.bindParams != nil {
		s.bindParams.Release()
		s.bindParams = nil
	}
	if s.bindStream != nil {
		s.bindStream.Release()
		s.bindStream = nil
	}
	return nil
}

func (s *statementImpl) closePrepared() {
	if s.prepared != nil {
		s.prepared.Close()
		s.prepared = nil
	}
}

// =============================================================================
// oracleRecordReader — implements driverbase.RecordReaderImpl
// =============================================================================

type oracleRecordReader struct {
	rows     *sql.Rows
	colTypes []*sql.ColumnType
	scanDest []interface{}
	scanVals []interface{}
	geomIndices []int
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

	r.firstRow = vals
	r.hasFirstRow = true

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

// =============================================================================
// Type mapping
// =============================================================================

var timestampNoTZ = &arrow.TimestampType{Unit: arrow.Microsecond}

func oracleTypeToArrow(ct *sql.ColumnType) arrow.DataType {
	dbType := strings.ToUpper(ct.DatabaseTypeName())

	switch {
	case dbType == "NUMBER":
		precision, scale, ok := ct.DecimalSize()
		if ok && scale == 0 && precision > 0 && precision <= 18 {
			return arrow.PrimitiveTypes.Int64
		}
		if ok && scale == 0 && precision > 18 {
			return arrow.BinaryTypes.String
		}
		return arrow.PrimitiveTypes.Float64
	case dbType == "FLOAT" || dbType == "BINARY_FLOAT" || dbType == "BINARY_DOUBLE":
		return arrow.PrimitiveTypes.Float64

	case dbType == "VARCHAR2" || dbType == "VARCHAR" || dbType == "NVARCHAR2" ||
		dbType == "CHAR" || dbType == "NCHAR" || dbType == "CLOB" || dbType == "NCLOB" ||
		dbType == "LONG" || dbType == "ROWID":
		return arrow.BinaryTypes.String

	case dbType == "DATE":
		return timestampNoTZ
	case dbType == "TIMESTAMPDTY" || dbType == "TIMESTAMP":
		return timestampNoTZ
	case dbType == "TIMESTAMPTZ_DTY" || dbType == "TIMESTAMPLTZ_DTY" ||
		strings.HasPrefix(dbType, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us

	case dbType == "INTERVALYM_DTY" || dbType == "INTERVALDS_DTY":
		return arrow.BinaryTypes.String

	case dbType == "RAW" || dbType == "LONG RAW" || dbType == "BLOB":
		return arrow.BinaryTypes.Binary

	case dbType == "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean

	default:
		return arrow.BinaryTypes.String
	}
}
