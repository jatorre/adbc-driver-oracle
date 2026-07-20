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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"
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
	ingestMode        string
	ingestGeomCols    map[int]int64 // col index → srid for geometry columns

	// String-column DDL sizing (see resolveColumnTypes)
	ingestStringType    string          // "auto" (default), "varchar2", "clob"
	ingestClobColumns   map[string]bool // uppercased column names forced to CLOB
	ingestScanLimitByte int64           // max bytes buffered for the auto type scan (0 = scan disabled)
	ingestScanLimitSet  bool            // distinguishes "unset, use default" from an explicit 0

	// inlineLimit is the VARCHAR2/CLOB byte boundary for this ingest, resolved
	// from the server's MAX_STRING_SIZE (4000 STANDARD / 32767 EXTENDED) once
	// executeBulkIngest starts. Threaded into resolveColumnTypes (DDL) and
	// stringColumnToSlice (bind type) so both agree per column.
	inlineLimit int
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
	case OptionIngestStringType:
		v := strings.ToLower(strings.TrimSpace(value))
		switch v {
		case "", "auto", "varchar2", "clob":
			s.ingestStringType = v
			return nil
		}
		return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
			"%s must be one of auto, varchar2, clob (got %q)", OptionIngestStringType, value)
	case OptionIngestClobColumns:
		s.ingestClobColumns = make(map[string]bool)
		for _, name := range strings.Split(value, ",") {
			if name = strings.TrimSpace(name); name != "" {
				s.ingestClobColumns[strings.ToUpper(name)] = true
			}
		}
		return nil
	case OptionIngestTypeScanLimit:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < 0 {
			return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument,
				"%s must be a non-negative integer (got %q; 0 disables the scan)", OptionIngestTypeScanLimit, value)
		}
		s.ingestScanLimitByte = n
		s.ingestScanLimitSet = true
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
	case OptionIngestStringType:
		if s.ingestStringType == "" {
			return "auto", nil
		}
		return s.ingestStringType, nil
	case OptionIngestClobColumns:
		cols := make([]string, 0, len(s.ingestClobColumns))
		for name := range s.ingestClobColumns {
			cols = append(cols, name)
		}
		sort.Strings(cols)
		return strings.Join(cols, ","), nil
	case OptionIngestTypeScanLimit:
		if !s.ingestScanLimitSet {
			return strconv.FormatInt(defaultTypeScanLimitBytes, 10), nil
		}
		return strconv.FormatInt(s.ingestScanLimitByte, 10), nil
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
	// driverbase-go v0.0.0-20260409+ added a *slog.Logger as the 3rd
	// argument to BaseRecordReader.Init (previously just (ctx, alloc,
	// params, options, impl)). Pass slog.Default() — driverbase rejects
	// a nil logger.
	if err := rr.Init(ctx, s.alloc, slog.Default(), nil, driverbase.BaseRecordReaderOptions{}, impl); err != nil {
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

	exec := func(args []interface{}) (int64, error) {
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

	// No parameters bound: execute once.
	if s.bindParams == nil || s.bindParams.NumCols() == 0 {
		return exec(nil)
	}
	// A bound batch with zero rows executes zero times.
	if s.bindParams.NumRows() == 0 {
		return 0, nil
	}

	// ADBC semantics: a bound batch executes the statement once per row.
	var total int64
	for row := 0; row < int(s.bindParams.NumRows()); row++ {
		affected, err := exec(s.extractBindArgs(row))
		if err != nil {
			return total, err
		}
		total += affected
	}
	return total, nil
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
	case *array.Int8:
		return int64(a.Value(idx))
	case *array.Int16:
		return int64(a.Value(idx))
	case *array.Int32:
		return int64(a.Value(idx))
	case *array.Int64:
		return a.Value(idx)
	case *array.Uint8:
		return int64(a.Value(idx))
	case *array.Uint16:
		return int64(a.Value(idx))
	case *array.Uint32:
		return int64(a.Value(idx))
	case *array.Uint64:
		// Route through decimal text — uint64 can exceed int64.
		return strconv.FormatUint(a.Value(idx), 10)
	case *array.Float32:
		return float64(a.Value(idx))
	case *array.Float64:
		return a.Value(idx)
	case *array.Decimal128:
		return a.Value(idx).ToString(a.DataType().(*arrow.Decimal128Type).Scale)
	case *array.Decimal256:
		return a.Value(idx).ToString(a.DataType().(*arrow.Decimal256Type).Scale)
	case *array.String:
		return a.Value(idx)
	case *array.LargeString:
		return a.Value(idx)
	case *array.Binary:
		return a.Value(idx)
	case *array.LargeBinary:
		return a.Value(idx)
	case *array.Boolean:
		return a.Value(idx)
	case *array.Timestamp:
		dt := a.DataType().(*arrow.TimestampType)
		if toTime, err := dt.GetToTimeFunc(); err == nil {
			return toTime(a.Value(idx))
		}
		return a.Value(idx).ToTime(dt.Unit)
	case *array.Date32:
		return a.Value(idx).ToTime()
	case *array.Date64:
		return a.Value(idx).ToTime()
	default:
		// Fallback: use the string representation
		return arr.ValueStr(idx)
	}
}

// --- Bulk Ingest ---

func (s *statementImpl) executeBulkIngest(ctx context.Context) (int64, error) {
	tableName := strings.ToUpper(s.ingestTargetTable)

	// Resolve the VARCHAR2/CLOB boundary from the server's MAX_STRING_SIZE once,
	// up front: both the DDL (resolveColumnTypes) and the per-batch bind type
	// (stringColumnToSlice) must use the same limit or a VARCHAR2 column could
	// receive a CLOB bind (or vice versa).
	s.inlineLimit = s.cnxn.inlineStringLimit(ctx)

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

	// Resolve per-column Oracle DDL types. In "auto" mode, sizing string
	// columns requires looking at the data: VARCHAR2 caps at 4000 bytes, so
	// wider values must land in CLOB (ORA-12899 otherwise). For a bound
	// stream, buffer records up to the scan limit while scanning widths;
	// buffered records are inserted first, then the rest of the stream.
	var buffered []arrow.RecordBatch
	bufferedOwned := true // until handed to the insert pipeline
	defer func() {
		if bufferedOwned {
			for _, rec := range buffered {
				rec.Release()
			}
		}
	}()

	var colTypes []string
	if mode != adbc.OptionValueIngestModeAppend {
		var widths []int
		if s.needsStringScan(schema) {
			widths = make([]int, schema.NumFields())
			limit := s.ingestScanLimitByte
			if !s.ingestScanLimitSet {
				limit = defaultTypeScanLimitBytes
			}
			// Same source precedence as the insert loop below: a bound stream
			// wins over bound params, so we always size from the data we insert.
			switch {
			case s.bindStream == nil:
				scanStringWidths(s.bindParams, widths)
			case limit == 0:
				// Scan explicitly disabled — fall back to data-independent
				// defaults (LargeString → CLOB, String → VARCHAR2).
				widths = nil
			default:
				var seen int64
				for seen < limit && s.bindStream.Next() {
					rec := s.bindStream.Record()
					rec.Retain()
					buffered = append(buffered, rec)
					seen += recordSizeEstimate(rec)
					scanStringWidths(rec, widths)
				}
				// Only io errors are visible here; end-of-stream is fine
				// (small streams fit entirely inside the scan window).
				if err := s.bindStream.Err(); err != nil {
					return -1, s.ErrorHelper.Errorf(adbc.StatusIO, "bind stream error: %s", err)
				}
			}
		}
		colTypes = resolveColumnTypes(schema, widths, s.ingestStringType, s.ingestClobColumns, s.inlineLimit)
		for i, f := range schema.Fields() {
			if colTypes[i] == "CLOB" && s.cnxn.Logger != nil {
				s.cnxn.Logger.Info("oracle ingest: string column mapped to CLOB",
					"table", tableName, "column", strings.ToUpper(f.Name))
			}
		}
	}

	// Handle table creation/replacement based on mode
	if err := s.prepareIngestTable(ctx, tableName, schema, mode, colTypes); err != nil {
		return -1, err
	}

	// Build INSERT statement.
	// Geometry columns are converted client-side from WKB to SdoGeometry UDT
	// structs and sent via go-ora's Object encoding (see wkbColumnToSdoSliceRange).
	colNames := make([]string, schema.NumFields())
	placeholders := make([]string, schema.NumFields())
	geomCols := make(map[int]int64) // col index → srid
	for i, f := range schema.Fields() {
		colNames[i] = fmt.Sprintf(`"%s"`, strings.ToUpper(f.Name))
		placeholders[i] = fmt.Sprintf(":%d", i+1)
		if isGeometryColumn(f) {
			geomCols[i] = extractSRIDFromField(f)
		}
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	s.ingestGeomCols = geomCols

	// Enable NOLOGGING on the table to reduce redo log overhead during bulk insert.
	// Note: APPEND_VALUES hint is not compatible with go-ora's array binding protocol
	// (Oracle interprets al8i4[1]>1 as batch execution, triggering ORA-38910).
	// NOLOGGING alone still reduces redo I/O for conventional inserts.
	s.cnxn.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s NOLOGGING", tableName))
	// Restore logging on every exit path, not just success.
	defer s.cnxn.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s LOGGING", tableName))

	var totalRows int64

	if s.bindStream != nil {
		// Pipeline: prepare next batch while current one is inserting.
		// This overlaps CPU-bound column conversion with network-bound Oracle insert.
		convert := func(rec arrow.RecordBatch) preparedBatchT {
			nrow := int(rec.NumRows())
			args := make([]interface{}, int(rec.NumCols()))
			for col := 0; col < int(rec.NumCols()); col++ {
				arr := rec.Column(col)
				if srid, ok := s.ingestGeomCols[col]; ok {
					// Convert WKB to SdoGeometry UDT objects client-side.
					// This also copies the data, so Arrow buffer reuse is safe.
					args[col] = wkbColumnToSdoSliceRange(arr, 0, nrow, srid)
				} else {
					args[col] = arrowColumnToSliceRange(arr, 0, nrow, s.inlineLimit)
				}
			}
			return preparedBatchT{args: args, nrow: nrow}
		}

		workers := s.streamWorkerCount()
		ahead := make(chan preparedBatchT, workers*2)
		done := make(chan struct{})
		var doneOnce sync.Once
		closeDone := func() { doneOnce.Do(func() { close(done) }) }
		prodExited := make(chan struct{})
		// Join the producer before returning (defers run LIFO: closeDone
		// unblocks it first). Otherwise an early error return would leave the
		// goroutine using bindStream while the caller Closes or re-executes
		// the statement.
		defer func() { <-prodExited }()
		defer closeDone()
		bufferedOwned = false // producer goroutine releases buffered records
		go func() {
			defer close(prodExited)
			defer close(ahead)
			send := func(rec arrow.RecordBatch) bool {
				if rec.NumRows() == 0 {
					return true
				}
				pb := convert(rec)
				select {
				case ahead <- pb:
					return true
				case <-done:
					// Consumer bailed out (insert error); stop converting so
					// this goroutine doesn't block forever on a full channel.
					return false
				}
			}
			for i, rec := range buffered {
				ok := send(rec)
				rec.Release()
				buffered[i] = nil
				if !ok {
					for _, r := range buffered[i+1:] {
						r.Release()
					}
					return
				}
			}
			for s.bindStream.Next() {
				if !send(s.bindStream.Record()) {
					return
				}
			}
		}()

		// Consumers: fan the stream across `workers` connections. Inserts are
		// order-independent; the round-trip latency (prepare/exec/commit) of one
		// worker overlaps another's, which is where the wall-clock win comes
		// from on WAN links. Errors close `done` (stops the producer) and the
		// remaining workers drain ahead via the closed channel.
		var (
			wg       sync.WaitGroup
			rowsIns  int64
			firstErr error
			errOnce  sync.Once
		)
		stopOnErr := func(err error) {
			errOnce.Do(func() {
				firstErr = err
				// Producer watches `done`; closing it here (not just via the
				// defer) stops conversion as soon as any worker fails.
				closeDone()
			})
		}
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n, err := s.streamInsertWorker(ctx, insertSQL, ahead, done)
				atomic.AddInt64(&rowsIns, n)
				if err != nil {
					stopOnErr(err)
				}
			}()
		}
		wg.Wait()
		totalRows = atomic.LoadInt64(&rowsIns)
		if firstErr != nil {
			return totalRows, firstErr
		}
		if err := s.bindStream.Err(); err != nil {
			return totalRows, s.ErrorHelper.Errorf(adbc.StatusIO, "bind stream error: %s", err)
		}
	} else if s.bindParams != nil {
		n, err := s.insertBatchParallel(ctx, insertSQL, s.bindParams)
		if err != nil {
			return 0, err
		}
		totalRows = n
	}

	return totalRows, nil
}

// insertBatchParallel splits a record batch into chunks and inserts them
// in parallel across multiple database connections for higher throughput.
func (s *statementImpl) insertBatchParallel(ctx context.Context, insertSQL string, rec arrow.RecordBatch) (int64, error) {
	numRows := int(rec.NumRows())
	if numRows == 0 {
		return 0, nil
	}

	// Determine parallelism — default 1 worker (single big batch is fastest on small instances).
	// Configure via oracle.ingest_workers option or ORACLE_INGEST_WORKERS env var.
	numWorkers := s.cnxn.ingestWorkers
	if numWorkers <= 0 {
		numWorkers = 1
	}
	if w := os.Getenv("ORACLE_INGEST_WORKERS"); w != "" {
		fmt.Sscanf(w, "%d", &numWorkers)
	}
	chunkSize := (numRows + numWorkers - 1) / numWorkers
	if chunkSize < 500 || numWorkers <= 1 {
		// For small batches, just use single connection
		return s.insertChunk(ctx, insertSQL, rec, 0, numRows)
	}

	type result struct {
		rows int64
		err  error
	}

	results := make(chan result, numWorkers)
	for w := 0; w < numWorkers; w++ {
		startRow := w * chunkSize
		endRow := startRow + chunkSize
		if endRow > numRows {
			endRow = numRows
		}
		if startRow >= numRows {
			break
		}

		go func(start, end int) {
			n, err := s.insertChunk(ctx, insertSQL, rec, start, end)
			results <- result{n, err}
		}(startRow, endRow)
	}

	var totalRows int64
	var firstErr error
	activeWorkers := numWorkers
	if numRows < chunkSize*numWorkers {
		activeWorkers = (numRows + chunkSize - 1) / chunkSize
	}
	for i := 0; i < activeWorkers; i++ {
		r := <-results
		totalRows += r.rows
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
	}

	if firstErr != nil {
		return totalRows, firstErr
	}
	return totalRows, nil
}

// insertChunk inserts a slice of rows [startRow, endRow) from a record batch.
// Uses a fresh connection from the pool to enable parallel inserts and avoid
// Oracle ORA-00600 errors from long-running UDT sessions.
func (s *statementImpl) insertChunk(ctx context.Context, insertSQL string, rec arrow.RecordBatch, startRow, endRow int) (int64, error) {
	numRows := endRow - startRow
	numCols := int(rec.NumCols())

	// Each goroutine gets its own connection from the pool.
	// This also works around Oracle ORA-00600 kopi2_readlen083 bugs
	// that occur after inserting many SDO_GEOMETRY UDTs on a single session.
	conn, err := s.cnxn.db.Conn(ctx)
	if err != nil {
		return 0, s.ErrorHelper.Errorf(adbc.StatusIO, "get connection failed: %s", err)
	}
	defer conn.Close()

	stmt, err := conn.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, s.ErrorHelper.Errorf(adbc.StatusIO, "prepare insert failed: %s", err)
	}
	defer stmt.Close()

	// Build columnar arrays for the chunk.
	// Geometry columns are converted client-side from WKB to SdoGeometry UDT objects.
	args := make([]interface{}, numCols)
	for col := 0; col < numCols; col++ {
		arr := rec.Column(col)
		if srid, ok := s.ingestGeomCols[col]; ok {
			args[col] = wkbColumnToSdoSliceRange(arr, startRow, endRow, srid)
		} else {
			args[col] = arrowColumnToSliceRange(arr, startRow, endRow, s.inlineLimit)
		}
	}

	_, err = stmt.ExecContext(ctx, args...)
	if err != nil {
		return 0, s.ErrorHelper.Errorf(adbc.StatusIO, "batch insert failed: %s", err)
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return int64(numRows), s.ErrorHelper.Errorf(adbc.StatusIO, "commit failed: %s", err)
	}

	return int64(numRows), nil
}

// insertPreparedBatch inserts pre-converted column args into Oracle.
// Used by the pipeline path where column conversion happens in a background goroutine.
// preparedBatchT is one converted record batch queued for insert: columnar
// go-ora bind args plus the row count they carry.
type preparedBatchT struct {
	args []interface{}
	nrow int
}

// streamWorkerCount resolves the stream-consumer parallelism: the
// oracle.ingest_workers option / ORACLE_INGEST_WORKERS env when set, else 4.
// Stream inserts are round-trip-bound (prepare/exec/commit to a remote
// listener), so overlapping a few connections is a wall-clock win even on
// small instances; 4 keeps contention modest on 1-ECPU Autonomous tiers.
func (s *statementImpl) streamWorkerCount() int {
	n := s.cnxn.ingestWorkers
	if w := os.Getenv("ORACLE_INGEST_WORKERS"); w != "" {
		fmt.Sscanf(w, "%d", &n)
	}
	if n <= 0 {
		n = 4
	}
	return n
}

// Stream-consumer tuning. Commits are batched (an Oracle round trip each) and
// sessions are recycled periodically: go-ora sessions that bind many
// SDO_GEOMETRY UDTs eventually hit ORA-00600 [kopi2_readlen083] (see
// insertChunk), so a worker re-connects every streamRecycleBatches batches
// instead of holding one session for the whole stream.
const (
	streamCommitEveryBatches = 16
	streamRecycleBatches     = 64
)

// streamInsertWorker drains prepared batches from `in` over its own pooled
// connection: prepare once per session, exec per batch, COMMIT every
// streamCommitEveryBatches, recycle the session every streamRecycleBatches.
// Returns the rows successfully inserted AND committed.
func (s *statementImpl) streamInsertWorker(ctx context.Context, insertSQL string, in <-chan preparedBatchT, done <-chan struct{}) (int64, error) {
	var (
		conn         *sql.Conn
		stmt         *sql.Stmt
		committed    int64
		uncommitted  int64
		sinceCommit  int
		sinceRecycle int
	)
	cleanup := func() {
		if stmt != nil {
			stmt.Close()
			stmt = nil
		}
		if conn != nil {
			conn.Close()
			conn = nil
		}
	}
	defer cleanup()

	open := func() error {
		var err error
		if conn, err = s.cnxn.db.Conn(ctx); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusIO, "get connection failed: %s", err)
		}
		if stmt, err = conn.PrepareContext(ctx, insertSQL); err != nil {
			cleanup()
			return s.ErrorHelper.Errorf(adbc.StatusIO, "prepare insert failed: %s", err)
		}
		sinceRecycle = 0
		return nil
	}
	commit := func() error {
		if conn == nil || uncommitted == 0 {
			return nil
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusIO, "commit failed: %s", err)
		}
		committed += uncommitted
		uncommitted = 0
		sinceCommit = 0
		return nil
	}

	for {
		var (
			pb preparedBatchT
			ok bool
		)
		select {
		case pb, ok = <-in:
		case <-done:
			// Another worker failed; commit what this worker already wrote
			// (matches the previous per-batch-commit semantics) and stop.
			err := commit()
			return committed, err
		}
		if !ok {
			err := commit()
			return committed, err
		}
		if conn == nil {
			if err := open(); err != nil {
				return committed, err
			}
		}
		if _, err := stmt.ExecContext(ctx, pb.args...); err != nil {
			commitErr := commit() // keep completed batches, like the old path
			_ = commitErr
			if isOraCode(err, 12899) {
				// A string wider than the scan window predicted slipped past
				// the auto sizing; tell the caller how to force CLOB.
				return committed, s.ErrorHelper.Errorf(adbc.StatusIO,
					"batch insert failed: %s (hint: value exceeds the column width chosen by the "+
						"auto type scan — set %s for the affected column, or %s=clob)",
					err, OptionIngestClobColumns, OptionIngestStringType)
			}
			return committed, s.ErrorHelper.Errorf(adbc.StatusIO, "batch insert failed: %s", err)
		}
		uncommitted += int64(pb.nrow)
		sinceCommit++
		sinceRecycle++
		if sinceCommit >= streamCommitEveryBatches {
			if err := commit(); err != nil {
				return committed, err
			}
		}
		if sinceRecycle >= streamRecycleBatches {
			if err := commit(); err != nil {
				return committed, err
			}
			cleanup()
		}
	}
}

func (s *statementImpl) insertPreparedBatch(ctx context.Context, insertSQL string, args []interface{}, numRows int) (int64, error) {
	conn, err := s.cnxn.db.Conn(ctx)
	if err != nil {
		return 0, s.ErrorHelper.Errorf(adbc.StatusIO, "get connection failed: %s", err)
	}
	defer conn.Close()

	stmt, err := conn.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, s.ErrorHelper.Errorf(adbc.StatusIO, "prepare insert failed: %s", err)
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, args...)
	if err != nil {
		if isOraCode(err, 12899) {
			// A string wider than the scan window predicted slipped past the
			// auto sizing; tell the caller how to force CLOB.
			return 0, s.ErrorHelper.Errorf(adbc.StatusIO,
				"batch insert failed: %s (hint: value exceeds the column width chosen by the "+
					"auto type scan — set %s for the affected column, or %s=clob)",
				err, OptionIngestClobColumns, OptionIngestStringType)
		}
		return 0, s.ErrorHelper.Errorf(adbc.StatusIO, "batch insert failed: %s", err)
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return int64(numRows), s.ErrorHelper.Errorf(adbc.StatusIO, "commit failed: %s", err)
	}

	return int64(numRows), nil
}

// arrowColumnToSliceRange extracts a range [start, end) from an Arrow column
// into a Go slice for go-ora's bulk array bind.
//
// Values must be bound in the driver-native representation, not stringified:
// stringified timestamps hit Oracle's implicit TO_DATE with an NLS format mask
// and fail on the ISO-8601 'T' separator (ORA-01858); stringified booleans
// ("true") fail NUMBER(1) conversion (ORA-01722).
//
// Everything variable-length is deep-copied: Arrow Value() calls alias the
// underlying buffer, which is released when the next stream record arrives.
// Holding the alias past that point produces use-after-free corruption
// (manifests as garbled VARCHAR2 columns through Oracle's bulk array bind).
func arrowColumnToSliceRange(arr arrow.Array, start, end, inlineLimit int) interface{} {
	numRows := end - start
	switch a := arr.(type) {
	case *array.Int64:
		return intColumnToSlice(a, start, end, func(v int64) int64 { return v })
	case *array.Int32:
		return intColumnToSlice(a, start, end, func(v int32) int64 { return int64(v) })
	case *array.Int16:
		return intColumnToSlice(a, start, end, func(v int16) int64 { return int64(v) })
	case *array.Int8:
		return intColumnToSlice(a, start, end, func(v int8) int64 { return int64(v) })
	case *array.Uint32:
		return intColumnToSlice(a, start, end, func(v uint32) int64 { return int64(v) })
	case *array.Uint16:
		return intColumnToSlice(a, start, end, func(v uint16) int64 { return int64(v) })
	case *array.Uint8:
		return intColumnToSlice(a, start, end, func(v uint8) int64 { return int64(v) })
	case *array.Uint64:
		// uint64 can exceed int64; bind the decimal text and let NUMBER(20)
		// take it exactly.
		vals := make([]sql.NullString, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullString{String: strconv.FormatUint(a.Value(i), 10), Valid: true}
			}
		}
		return vals
	case *array.Float64:
		vals := make([]sql.NullFloat64, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullFloat64{Float64: a.Value(i), Valid: true}
			}
		}
		return vals
	case *array.Float32:
		vals := make([]sql.NullFloat64, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullFloat64{Float64: float64(a.Value(i)), Valid: true}
			}
		}
		return vals
	case *array.Boolean:
		// NUMBER(1) column, bound as explicit 0/1 (stringified "true"/"false"
		// would fail the implicit NUMBER conversion with ORA-01722).
		vals := make([]sql.NullInt64, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				var v int64
				if a.Value(i) {
					v = 1
				}
				vals[i-start] = sql.NullInt64{Int64: v, Valid: true}
			}
		}
		return vals
	case *array.String:
		return stringColumnToSlice(a, start, end, inlineLimit)
	case *array.LargeString:
		return stringColumnToSlice(a, start, end, inlineLimit)
	case *array.Binary:
		return binaryColumnToSlice(a, start, end)
	case *array.LargeBinary:
		return binaryColumnToSlice(a, start, end)
	case *array.FixedSizeBinary:
		return binaryColumnToSlice(a, start, end)
	case *array.BinaryView:
		return binaryColumnToSlice(a, start, end)
	case *array.Decimal128:
		vals := make([]sql.NullString, numRows)
		scale := a.DataType().(*arrow.Decimal128Type).Scale
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullString{String: a.Value(i).ToString(scale), Valid: true}
			}
		}
		return vals
	case *array.Decimal256:
		vals := make([]sql.NullString, numRows)
		scale := a.DataType().(*arrow.Decimal256Type).Scale
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = sql.NullString{String: a.Value(i).ToString(scale), Valid: true}
			}
		}
		return vals
	case *array.Timestamp:
		return timestampColumnToSlice(a, start, end)
	case *array.Date32:
		vals := make([]go_ora.NullTimeStamp, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = go_ora.NullTimeStamp{TimeStamp: go_ora.TimeStamp(a.Value(i).ToTime()), Valid: true}
			}
		}
		return vals
	case *array.Date64:
		vals := make([]go_ora.NullTimeStamp, numRows)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = go_ora.NullTimeStamp{TimeStamp: go_ora.TimeStamp(a.Value(i).ToTime()), Valid: true}
			}
		}
		return vals
	case *array.StringView:
		return stringColumnToSlice(a, start, end, inlineLimit)
	default:
		// Remaining types (Time32/64, Duration, Interval, Dictionary, nested…)
		// have no natural Oracle mapping; bind their textual form into the
		// VARCHAR2 fallback column. Clone defensively: some ValueStr
		// implementations (Dictionary, RunEndEncoded) alias the Arrow buffer.
		vals := make([]sql.NullString, numRows)
		for i := start; i < end; i++ {
			if !arr.IsNull(i) {
				vals[i-start] = sql.NullString{String: strings.Clone(arr.ValueStr(i)), Valid: true}
			}
		}
		return vals
	}
}

// intColumnToSlice converts any fixed-width integer Arrow array to NullInt64s.
func intColumnToSlice[T int64 | int32 | int16 | int8 | uint32 | uint16 | uint8](
	a interface {
		arrow.Array
		Value(int) T
	}, start, end int, conv func(T) int64,
) []sql.NullInt64 {
	vals := make([]sql.NullInt64, end-start)
	for i := start; i < end; i++ {
		if !a.IsNull(i) {
			vals[i-start] = sql.NullInt64{Int64: conv(a.Value(i)), Valid: true}
		}
	}
	return vals
}

// stringColumnToSlice deep-copies string values (see aliasing note above).
//
// Batches whose widest value exceeds the VARCHAR2 cap bind as go_ora.Clob:
// plain string binds ride go-ora's negotiated varchar limit (4000 bytes on
// MAX_STRING_SIZE=STANDARD servers, 32767 on EXTENDED), so a wide value could
// fail at bind time even though the destination column is CLOB. The bind type
// is chosen per batch — narrow batches keep the cheaper varchar bind, which
// Oracle implicitly converts into CLOB columns.
func stringColumnToSlice(a interface {
	arrow.Array
	Value(int) string
}, start, end, inlineLimit int) interface{} {
	vals := make([]sql.NullString, end-start)
	maxWidth := 0
	for i := start; i < end; i++ {
		if !a.IsNull(i) {
			v := strings.Clone(a.Value(i))
			if len(v) > maxWidth {
				maxWidth = len(v)
			}
			vals[i-start] = sql.NullString{String: v, Valid: true}
		}
	}
	if maxWidth <= inlineLimit {
		return vals
	}
	lobs := make([]go_ora.Clob, len(vals))
	for i, v := range vals {
		lobs[i] = go_ora.Clob{String: v.String, Valid: v.Valid}
	}
	return lobs
}

// binaryColumnToSlice deep-copies binary values (see aliasing note above).
func binaryColumnToSlice(a interface {
	arrow.Array
	Value(int) []byte
}, start, end int) [][]byte {
	vals := make([][]byte, end-start)
	for i := start; i < end; i++ {
		if !a.IsNull(i) {
			src := a.Value(i)
			dst := make([]byte, len(src))
			copy(dst, src)
			vals[i-start] = dst
		}
	}
	return vals
}

// timestampColumnToSlice binds timestamps in the driver's native temporal form.
//
// Naive (no time zone) columns bind as go_ora.TimeStamp — wire-level Oracle
// TIMESTAMP carrying the wall-clock fields — so the value lands verbatim with
// no session-time-zone conversion. Zone-aware columns bind as time.Time, which
// go-ora encodes as TIMESTAMP WITH TIME ZONE, preserving the absolute instant.
func timestampColumnToSlice(a *array.Timestamp, start, end int) interface{} {
	dt := a.DataType().(*arrow.TimestampType)
	toTime, err := dt.GetToTimeFunc()
	if err != nil {
		// Unloadable time zone — fall back to UTC-based conversion.
		unit := dt.Unit
		toTime = func(ts arrow.Timestamp) time.Time { return ts.ToTime(unit) }
	}

	if dt.TimeZone == "" {
		vals := make([]go_ora.NullTimeStamp, end-start)
		for i := start; i < end; i++ {
			if !a.IsNull(i) {
				vals[i-start] = go_ora.NullTimeStamp{TimeStamp: go_ora.TimeStamp(toTime(a.Value(i))), Valid: true}
			}
		}
		return vals
	}

	vals := make([]sql.NullTime, end-start)
	for i := start; i < end; i++ {
		if !a.IsNull(i) {
			vals[i-start] = sql.NullTime{Time: toTime(a.Value(i)), Valid: true}
		}
	}
	return vals
}

// wkbColumnToSdoSliceRange converts a range [start, end) of WKB binary values to SdoGeometry UDT objects.
func wkbColumnToSdoSliceRange(arr arrow.Array, start, end int, srid int64) interface{} {
	binArr, ok := arr.(*array.Binary)
	if !ok {
		return make([]*go_ora.Object, end-start)
	}

	vals := make([]*go_ora.Object, end-start)
	for i := start; i < end; i++ {
		if binArr.IsNull(i) {
			vals[i-start] = nil
			continue
		}
		wkb := binArr.Value(i)
		geom, err := WKBToSdo(wkb, srid)
		if err != nil {
			vals[i-start] = nil
			continue
		}
		vals[i-start] = go_ora.NewObject("MDSYS", "SDO_GEOMETRY", *geom)
	}
	return vals
}

func (s *statementImpl) prepareIngestTable(ctx context.Context, tableName string, schema *arrow.Schema, mode string, colTypes []string) error {
	switch mode {
	case adbc.OptionValueIngestModeCreate:
		// Create table — fail if exists
		ddl := buildCreateTableDDL(tableName, schema, colTypes)
		if _, err := s.cnxn.db.ExecContext(ctx, ddl); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusIO, "create table failed: %s", err)
		}
	case adbc.OptionValueIngestModeAppend:
		// Table must exist — do nothing
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate. A missing table (ORA-00942) is fine; any other
		// drop failure would surface as a confusing ORA-00955 on the CREATE,
		// so report it directly.
		if _, err := s.cnxn.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s PURGE", tableName)); err != nil && !isOraCode(err, 942) {
			return s.ErrorHelper.Errorf(adbc.StatusIO, "drop table failed: %s", err)
		}
		ddl := buildCreateTableDDL(tableName, schema, colTypes)
		if _, err := s.cnxn.db.ExecContext(ctx, ddl); err != nil {
			return s.ErrorHelper.Errorf(adbc.StatusIO, "create table failed: %s", err)
		}
	case adbc.OptionValueIngestModeCreateAppend:
		// Create if not exists, append if exists. Only ORA-00955 ("name is
		// already used") means "exists" — anything else is a real failure.
		ddl := buildCreateTableDDL(tableName, schema, colTypes)
		if _, err := s.cnxn.db.ExecContext(ctx, ddl); err != nil && !isOraCode(err, 955) {
			return s.ErrorHelper.Errorf(adbc.StatusIO, "create table failed: %s", err)
		}
	default:
		return s.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "unknown ingest mode: %s", mode)
	}
	return nil
}

// isOraCode reports whether err carries the given Oracle error code
// (e.g. 942 for "ORA-00942: table or view does not exist").
func isOraCode(err error, code int) bool {
	if err == nil {
		return false
	}
	var oraErr *network.OracleError
	if errors.As(err, &oraErr) {
		return oraErr.ErrCode == code
	}
	// database/sql can wrap the driver error in plain-text form.
	return strings.Contains(err.Error(), fmt.Sprintf("ORA-%05d", code))
}

// isGeometryColumn checks if an Arrow field is a geometry column.
// Detects via geoarrow.wkb extension metadata (from Arrow/ADBC) or
// GeoParquet column naming convention (GEOM, GEOMETRY, geom, geometry).
func isGeometryColumn(f arrow.Field) bool {
	if f.Type.ID() != arrow.BINARY {
		return false
	}
	// Check geoarrow.wkb extension metadata
	extName, ok := f.Metadata.GetValue("ARROW:extension:name")
	if ok && extName == "geoarrow.wkb" {
		return true
	}
	// Check common geometry column names (GeoParquet convention)
	name := strings.ToUpper(f.Name)
	return name == "GEOM" || name == "GEOMETRY" || name == "GEOM_WKB" || name == "WKB_GEOMETRY"
}

// extractSRIDFromField extracts the SRID from geoarrow extension metadata.
//
// DuckDB exposes CRS metadata as the compact GeoArrow form
//
//	{"crs":"EPSG:3857"}
//
// while other producers send PROJJSON
//
//	{"crs":{"id":{"authority":"EPSG","code":3857}}}
//
// Both are accepted. Returns 4326 when no usable SRID is found so that
// callers always have a concrete value to bind into SDO_GEOMETRY.
func extractSRIDFromField(f arrow.Field) int64 {
	meta, ok := f.Metadata.GetValue("ARROW:extension:metadata")
	if !ok || meta == "" || meta == "{}" {
		return 4326
	}
	var holder struct {
		CRS json.RawMessage `json:"crs"`
	}
	if err := json.Unmarshal([]byte(meta), &holder); err != nil || len(holder.CRS) == 0 || string(holder.CRS) == "null" {
		return 4326
	}
	// Compact GeoArrow CRS: a JSON string like "EPSG:3857".
	var crsStr string
	if err := json.Unmarshal(holder.CRS, &crsStr); err == nil {
		if strings.HasPrefix(strings.ToUpper(crsStr), "EPSG:") {
			if code, err := strconv.ParseInt(crsStr[5:], 10, 64); err == nil && code != 0 {
				return code
			}
		}
		return 4326
	}
	// PROJJSON: {"id":{"authority":"EPSG","code":N}}.
	var crs struct {
		ID struct {
			Authority string `json:"authority"`
			Code      int64  `json:"code"`
		} `json:"id"`
	}
	if err := json.Unmarshal(holder.CRS, &crs); err == nil {
		if strings.EqualFold(crs.ID.Authority, "EPSG") && crs.ID.Code != 0 {
			return crs.ID.Code
		}
	}
	return 4326
}

// maxInlineStringBytes is the VARCHAR2 byte cap with MAX_STRING_SIZE=STANDARD.
// String values wider than this must be stored as CLOB on a STANDARD server.
const maxInlineStringBytes = 4000

// maxExtendedStringBytes is the VARCHAR2 byte cap with MAX_STRING_SIZE=EXTENDED
// (the Autonomous DB default). Values up to this size bind as plain VARCHAR2
// (fast batch array bind) instead of per-row temporary-LOB CLOB binds.
const maxExtendedStringBytes = 32767

// defaultTypeScanLimitBytes bounds how much of a bound stream executeBulkIngest
// buffers while sizing string columns in "auto" mode (~64 MiB keeps a couple of
// wide DuckDB batches without holding the whole table in memory).
const defaultTypeScanLimitBytes = 64 << 20

// needsStringScan reports whether the schema has string columns whose DDL type
// depends on observed data widths (i.e. "auto" mode with nothing forcing CLOB).
func (s *statementImpl) needsStringScan(schema *arrow.Schema) bool {
	if s.ingestStringType == "varchar2" || s.ingestStringType == "clob" {
		return false
	}
	for _, f := range schema.Fields() {
		id := f.Type.ID()
		if (id == arrow.STRING || id == arrow.STRING_VIEW || id == arrow.LARGE_STRING) &&
			!s.ingestClobColumns[strings.ToUpper(f.Name)] {
			return true
		}
	}
	return false
}

// scanStringWidths updates widths[i] with the maximum value byte-length seen
// in rec for each string column. len(Value(i)) reads the offsets only — no
// value bytes are copied.
func scanStringWidths(rec arrow.RecordBatch, widths []int) {
	for col := 0; col < int(rec.NumCols()) && col < len(widths); col++ {
		switch a := rec.Column(col).(type) {
		case *array.String:
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					if n := len(a.Value(i)); n > widths[col] {
						widths[col] = n
					}
				}
			}
		case *array.LargeString:
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					if n := len(a.Value(i)); n > widths[col] {
						widths[col] = n
					}
				}
			}
		case *array.StringView:
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					if n := len(a.Value(i)); n > widths[col] {
						widths[col] = n
					}
				}
			}
		}
	}
}

// recordSizeEstimate approximates the in-memory size of a record batch by
// summing its Arrow buffer lengths, recursing into child data (list/struct
// values, dictionaries) — top-level buffers alone are just validity+offsets
// for nested types and would undercount by orders of magnitude.
func recordSizeEstimate(rec arrow.RecordBatch) int64 {
	var total int64
	for col := 0; col < int(rec.NumCols()); col++ {
		total += arrayDataSize(rec.Column(col).Data())
	}
	return total
}

func arrayDataSize(data arrow.ArrayData) int64 {
	// Dictionary() and children can surface a typed-nil *array.Data (seen on
	// records imported through the C data interface) — the interface itself
	// is non-nil, so check the concrete pointer too.
	if data == nil {
		return 0
	}
	if d, ok := data.(*array.Data); ok && d == nil {
		return 0
	}
	var total int64
	for _, buf := range data.Buffers() {
		if buf != nil {
			total += int64(buf.Len())
		}
	}
	for _, child := range data.Children() {
		total += arrayDataSize(child)
	}
	total += arrayDataSize(data.Dictionary())
	return total
}

// resolveColumnTypes decides the Oracle DDL type for every column.
//
// String columns are the interesting case: VARCHAR2 caps at 4000 bytes, so
//   - stringType "clob" / "varchar2" forces that type for all string columns;
//   - clobColumns (uppercased names) forces CLOB for specific columns;
//   - otherwise ("auto") a column becomes CLOB when the scanned data contains
//     a value wider than 4000 bytes (widths[i], from scanStringWidths). With
//     no scan data at all, LargeString columns fall back to CLOB — producers
//     use the large layout precisely when values are big — while regular
//     strings stay VARCHAR2.
//
// widths may be nil when no scan ran (forced types, or scan disabled).
// inlineLimit is the VARCHAR2 byte cap for this server (maxInlineStringBytes on
// STANDARD, maxExtendedStringBytes on EXTENDED); columns wider than it become
// CLOB. It must match the value threaded into stringColumnToSlice so the DDL
// type and the per-batch bind type agree.
// varcharDeclWidth picks a VARCHAR2 declared width from the observed max byte
// width of a column's data. It adds headroom (values beyond the type-scan
// window aren't measured — same trust the CLOB threshold already relies on)
// and rounds up to a 64-byte bucket for stability across similar batches,
// clamped to [64, inlineLimit]. Kept tight because go-ora's read buffer scales
// with the declared width, not the actual value length.
func varcharDeclWidth(observed, inlineLimit int) int {
	if observed < 1 {
		observed = 1
	}
	n := ((observed*2 + 16 + 63) / 64) * 64 // 2x headroom, rounded to a 64-byte bucket
	// A column whose data fits inline (<= 4000 bytes) must stay declared inline
	// even after headroom — VARCHAR2 > 4000 stores out-of-line on EXTENDED
	// servers and reads slowly, the very thing this sizing avoids.
	if observed <= maxInlineStringBytes && n > maxInlineStringBytes {
		n = maxInlineStringBytes
	}
	if n < 64 {
		n = 64
	}
	if n > inlineLimit {
		n = inlineLimit
	}
	return n
}

func resolveColumnTypes(schema *arrow.Schema, widths []int, stringType string, clobColumns map[string]bool, inlineLimit int) []string {
	types := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		if isGeometryColumn(f) {
			types[i] = "MDSYS.SDO_GEOMETRY"
			continue
		}
		id := f.Type.ID()
		if id == arrow.STRING || id == arrow.LARGE_STRING || id == arrow.STRING_VIEW {
			switch {
			case clobColumns[strings.ToUpper(f.Name)] || stringType == "clob":
				types[i] = "CLOB"
			case stringType == "varchar2":
				types[i] = fmt.Sprintf("VARCHAR2(%d)", inlineLimit)
			case widths != nil && i < len(widths) && widths[i] > inlineLimit:
				types[i] = "CLOB"
			case widths != nil && i < len(widths):
				// Size to the observed max width, not the server inline limit.
				// go-ora array-fetch defines a per-row buffer sized to the
				// DECLARED VARCHAR2 width, so declaring VARCHAR2(32767) for a
				// column of short strings makes reads O(declared width)/row —
				// a full-table read of such a column is 30x+ slower and can
				// exceed query timeouts (Oracle-as-source transfers, exports).
				// Reads scale linearly with the declared width, so keep it tight.
				types[i] = fmt.Sprintf("VARCHAR2(%d)", varcharDeclWidth(widths[i], inlineLimit))
			case id == arrow.LARGE_STRING:
				types[i] = "CLOB"
			default:
				types[i] = fmt.Sprintf("VARCHAR2(%d)", inlineLimit)
			}
			continue
		}
		types[i] = arrowToOracleType(f.Type)
	}
	return types
}

// buildCreateTableDDL renders CREATE TABLE DDL. colTypes comes from
// resolveColumnTypes; pass nil to use data-independent defaults.
func buildCreateTableDDL(tableName string, schema *arrow.Schema, colTypes []string) string {
	if colTypes == nil {
		colTypes = resolveColumnTypes(schema, nil, "", nil, maxInlineStringBytes)
	}
	var cols []string
	for i, f := range schema.Fields() {
		nullable := ""
		// Oracle stores '' as NULL, so a NOT NULL string column would reject
		// legitimate empty strings — skip the constraint for string types.
		id := f.Type.ID()
		isString := id == arrow.STRING || id == arrow.LARGE_STRING || id == arrow.STRING_VIEW
		if !f.Nullable && !isString {
			nullable = " NOT NULL"
		}
		cols = append(cols, fmt.Sprintf(`"%s" %s%s`, strings.ToUpper(f.Name), colTypes[i], nullable))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s) NOLOGGING", tableName, strings.Join(cols, ", "))
}

// arrowToOracleType maps a non-string, non-geometry Arrow type to Oracle DDL.
// String columns are resolved by resolveColumnTypes, which sizes them from
// the data; the mapping here must stay bind-compatible with
// arrowColumnToSliceRange.
func arrowToOracleType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return "NUMBER(19)"
	case arrow.UINT8, arrow.UINT16, arrow.UINT32:
		return "NUMBER(19)"
	case arrow.UINT64:
		// uint64 max is 1.8e19 — one digit more than NUMBER(19) holds.
		return "NUMBER(20)"
	case arrow.FLOAT16, arrow.FLOAT32:
		return "BINARY_FLOAT"
	case arrow.FLOAT64:
		return "BINARY_DOUBLE"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		if dec, ok := dt.(arrow.DecimalType); ok {
			prec, scale := dec.GetPrecision(), dec.GetScale()
			// Oracle NUMBER(p,s) caps p at 38. Clamping would reject values
			// that legitimately fit the Arrow type (ORA-01438), so fall back
			// to unconstrained NUMBER for anything wider.
			if prec >= 1 && prec <= 38 && scale >= 0 && scale <= prec {
				return fmt.Sprintf("NUMBER(%d,%d)", prec, scale)
			}
		}
		return "NUMBER"
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return fmt.Sprintf("VARCHAR2(%d)", maxInlineStringBytes)
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.BINARY_VIEW:
		return "BLOB"
	case arrow.BOOL:
		return "NUMBER(1)"
	case arrow.TIMESTAMP:
		ts, ok := dt.(*arrow.TimestampType)
		if !ok {
			return "TIMESTAMP(6)"
		}
		prec := 6
		if ts.Unit == arrow.Nanosecond {
			prec = 9
		}
		if ts.TimeZone != "" {
			return fmt.Sprintf("TIMESTAMP(%d) WITH TIME ZONE", prec)
		}
		return fmt.Sprintf("TIMESTAMP(%d)", prec)
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	default:
		return fmt.Sprintf("VARCHAR2(%d)", maxInlineStringBytes)
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
	rows        *sql.Rows
	colTypes    []*sql.ColumnType
	scanDest    []interface{}
	scanVals    []interface{}
	geomIndices []int
	firstRow    []interface{}
	hasFirstRow bool
}

func (r *oracleRecordReader) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	colTypes, err := r.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	r.colTypes = colTypes

	fields := make([]arrow.Field, len(colTypes))
	r.geomIndices = nil

	// Detect geometry columns. go-ora reports SDO_GEOMETRY natively for the
	// MDSYS UDT registered in databaseImpl.getPool; XMLTYPE is also accepted
	// for the SDO_UTIL.TO_GMLGEOMETRY-style server-side conversion path.
	for i, ct := range colTypes {
		dbType := strings.ToUpper(ct.DatabaseTypeName())
		if dbType == "SDO_GEOMETRY" || dbType == "MDSYS.SDO_GEOMETRY" || dbType == "XMLTYPE" {
			r.geomIndices = append(r.geomIndices, i)
		}
	}

	// If there are geometry columns, peek at the first row to get the SRID
	var srid int64
	if len(r.geomIndices) > 0 {
		srid = r.peekSRID()
	}

	// Build schema
	geomCols := make(map[int]struct{}, len(r.geomIndices))
	for _, idx := range r.geomIndices {
		geomCols[idx] = struct{}{}
	}
	for i, ct := range colTypes {
		nullable, _ := ct.Nullable()

		if _, ok := geomCols[i]; ok {
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

// AppendRows appends one row per call. The new driverbase API (since
// 20260409) batches multiple rows per call to amortize builder overhead;
// we keep the per-row semantics by returning rowCount=1 each call.
func (r *oracleRecordReader) AppendRows(builder *array.RecordBuilder) (int64, int64, error) {
	// Replay peeked first row if available
	if r.hasFirstRow {
		r.hasFirstRow = false
		copy(r.scanVals, r.firstRow)
		r.firstRow = nil
	} else {
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				return 0, 0, err
			}
			return 0, 0, io.EOF
		}

		if err := r.rows.Scan(r.scanDest...); err != nil {
			return 0, 0, fmt.Errorf("scan error: %w", err)
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

	return 1, rowSize, nil
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

	// Note: go-ora's ColumnType.DatabaseTypeName returns TNS wire-type names
	// (NCHAR, IBDouble, OCIBlobLocator, TimeStampDTY, …), not the DDL names
	// from ALL_TAB_COLUMNS — match both here.
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
	case dbType == "FLOAT" || dbType == "BINARY_FLOAT" || dbType == "BINARY_DOUBLE" ||
		dbType == "IBFLOAT" || dbType == "IBDOUBLE" || // TNS names for BINARY_FLOAT/DOUBLE
		dbType == "BFLOAT" || dbType == "BDOUBLE":
		return arrow.PrimitiveTypes.Float64

	case dbType == "VARCHAR2" || dbType == "VARCHAR" || dbType == "NVARCHAR2" ||
		dbType == "CHAR" || dbType == "NCHAR" || dbType == "CLOB" || dbType == "NCLOB" ||
		dbType == "LONG" || dbType == "LONGVARCHAR" || dbType == "ROWID" || dbType == "UROWID" ||
		dbType == "OCICLOBLOCATOR":
		return arrow.BinaryTypes.String

	case dbType == "DATE":
		return timestampNoTZ
	case dbType == "TIMESTAMPDTY" || dbType == "TIMESTAMP":
		return timestampNoTZ
	case dbType == "TIMESTAMPTZ_DTY" || dbType == "TIMESTAMPLTZ_DTY" ||
		strings.HasPrefix(dbType, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us

	case strings.HasPrefix(dbType, "INTERVAL"):
		return arrow.BinaryTypes.String

	case dbType == "RAW" || dbType == "LONG RAW" || dbType == "LONGRAW" || dbType == "BLOB" ||
		dbType == "OCIBLOBLOCATOR" || dbType == "OCIFILELOCATOR":
		return arrow.BinaryTypes.Binary

	case dbType == "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean

	default:
		return arrow.BinaryTypes.String
	}
}
