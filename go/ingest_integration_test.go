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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// openTestConnection returns an ADBC connection to the integration database,
// plus a cleanup-friendly exec helper for raw SQL (DDL, verification queries).
func openTestConnection(t *testing.T) (adbc.Connection, func(query string, args ...any) *queryResult) {
	t.Helper()
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { cnxn.Close() })

	query := func(q string, args ...any) *queryResult {
		st, err := cnxn.NewStatement()
		if err != nil {
			return &queryResult{err: err}
		}
		defer st.Close()
		if err := st.SetSqlQuery(q); err != nil {
			return &queryResult{err: err}
		}
		reader, _, err := st.ExecuteQuery(ctx)
		if err != nil {
			return &queryResult{err: err}
		}
		defer reader.Release()
		res := &queryResult{}
		for reader.Next() {
			rec := reader.Record()
			for r := 0; r < int(rec.NumRows()); r++ {
				row := make([]string, rec.NumCols())
				for c := 0; c < int(rec.NumCols()); c++ {
					col := rec.Column(c)
					if col.IsNull(r) {
						row[c] = "<null>"
					} else {
						row[c] = col.ValueStr(r)
					}
				}
				res.rows = append(res.rows, row)
			}
		}
		res.err = reader.Err()
		return res
	}
	return cnxn, query
}

type queryResult struct {
	rows [][]string
	err  error
}

func execTestSQL(t *testing.T, cnxn adbc.Connection, q string) error {
	t.Helper()
	st, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.SetSqlQuery(q); err != nil {
		return err
	}
	_, err = st.ExecuteUpdate(context.Background())
	return err
}

func ingestStream(t *testing.T, cnxn adbc.Connection, table, mode string, opts map[string]string, schema *arrow.Schema, recs []arrow.RecordBatch) (int64, error) {
	t.Helper()
	rdr, err := array.NewRecordReader(schema, recs)
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()

	st, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer st.Close()

	gs := st.(adbc.GetSetOptions)
	if err := gs.SetOption(adbc.OptionKeyIngestTargetTable, table); err != nil {
		t.Fatalf("set target table: %v", err)
	}
	if err := gs.SetOption(adbc.OptionKeyIngestMode, mode); err != nil {
		t.Fatalf("set mode: %v", err)
	}
	for k, v := range opts {
		if err := gs.SetOption(k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	if err := st.BindStream(context.Background(), rdr); err != nil {
		t.Fatalf("BindStream: %v", err)
	}
	return st.ExecuteUpdate(context.Background())
}

// TestIntegration_IngestWideStrings covers the ORA-12899 scenario: BQ STRING
// values wider than 4000 bytes must land in CLOB automatically and round-trip
// verbatim (story sc-558620 acceptance criteria 1–2).
func TestIntegration_IngestWideStrings(t *testing.T) {
	cnxn, query := openTestConnection(t)
	table := "ADBC_TEST_WIDE_STR"
	execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE")
	t.Cleanup(func() { execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE") })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	wide := strings.Repeat("x", 28198) // width from the failing DO table
	medium := strings.Repeat("y", 8026)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{wide, medium, ""}, []bool{true, true, false})
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	affected, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeCreate, nil, schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if affected != 3 {
		t.Errorf("affected = %d, want 3", affected)
	}

	// DATA must be CLOB, NAME must stay VARCHAR2.
	res := query(`SELECT COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '` + table + `' ORDER BY COLUMN_ID`)
	if res.err != nil {
		t.Fatalf("column query: %v", res.err)
	}
	types := map[string]string{}
	for _, row := range res.rows {
		types[row[0]] = row[1]
	}
	if types["DATA"] != "CLOB" {
		t.Errorf("DATA type = %q, want CLOB", types["DATA"])
	}
	if types["NAME"] != "VARCHAR2" {
		t.Errorf("NAME type = %q, want VARCHAR2", types["NAME"])
	}

	// Round-trip byte lengths (acceptance: DBMS_LOB.GETLENGTH).
	res = query(`SELECT "ID", DBMS_LOB.GETLENGTH("DATA") FROM ` + table + ` ORDER BY "ID"`)
	if res.err != nil {
		t.Fatalf("length query: %v", res.err)
	}
	wantLens := []string{"28198", "8026", "<null>"}
	for i, row := range res.rows {
		if row[1] != wantLens[i] {
			t.Errorf("row %s DBMS_LOB.GETLENGTH = %s, want %s", row[0], row[1], wantLens[i])
		}
	}

	// Contents must round-trip verbatim through the driver's read path too.
	res = query(`SELECT "DATA" FROM ` + table + ` WHERE "ID" = 1`)
	if res.err != nil {
		t.Fatalf("readback query: %v", res.err)
	}
	if len(res.rows) != 1 || res.rows[0][0] != wide {
		got := ""
		if len(res.rows) == 1 {
			got = fmt.Sprintf("len %d, head %.20q", len(res.rows[0][0]), res.rows[0][0])
		}
		t.Errorf("CLOB readback mismatch: got %s, want len %d", got, len(wide))
	}
}

// TestIntegration_IngestTemporal covers the ORA-01858 scenario: TIMESTAMP,
// TIMESTAMP WITH TIME ZONE, and DATE columns bound natively (not as ISO-8601
// strings hitting an implicit TO_DATE).
func TestIntegration_IngestTemporal(t *testing.T) {
	cnxn, query := openTestConnection(t)
	table := "ADBC_TEST_TEMPORAL"
	execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE")
	t.Cleanup(func() { execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE") })

	tsType := &arrow.TimestampType{Unit: arrow.Microsecond}
	tstzType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: tsType, Nullable: true},
		{Name: "tstz", Type: tstzType, Nullable: true},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "ratio", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
	}, nil)

	naive := time.Date(2024, 3, 15, 10, 30, 45, 123456000, time.UTC)
	instant := time.Date(2024, 6, 1, 16, 0, 0, 500000000, time.UTC)
	day := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	tsVal, _ := arrow.TimestampFromTime(naive, arrow.Microsecond)
	b.Field(1).(*array.TimestampBuilder).Append(tsVal)
	b.Field(1).(*array.TimestampBuilder).AppendNull()
	tstzVal, _ := arrow.TimestampFromTime(instant, arrow.Microsecond)
	b.Field(2).(*array.TimestampBuilder).Append(tstzVal)
	b.Field(2).(*array.TimestampBuilder).AppendNull()
	b.Field(3).(*array.Date32Builder).Append(arrow.Date32FromTime(day))
	b.Field(3).(*array.Date32Builder).AppendNull()
	b.Field(4).(*array.BooleanBuilder).Append(true)
	b.Field(4).(*array.BooleanBuilder).Append(false)
	b.Field(5).(*array.Float32Builder).Append(2.5)
	b.Field(5).(*array.Float32Builder).AppendNull()
	rec := b.NewRecord()
	defer rec.Release()

	if _, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeCreate, nil, schema, []arrow.RecordBatch{rec}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Column DDL types
	res := query(`SELECT COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '` + table + `' ORDER BY COLUMN_ID`)
	if res.err != nil {
		t.Fatalf("column query: %v", res.err)
	}
	types := map[string]string{}
	for _, row := range res.rows {
		types[row[0]] = row[1]
	}
	for col, want := range map[string]string{
		"TS":    "TIMESTAMP(6)",
		"TSTZ":  "TIMESTAMP(6) WITH TIME ZONE",
		"D":     "DATE",
		"FLAG":  "NUMBER",
		"RATIO": "BINARY_FLOAT",
	} {
		if types[col] != want {
			t.Errorf("%s type = %q, want %q", col, types[col], want)
		}
	}

	// Values: format server-side to avoid client parsing ambiguity.
	res = query(`SELECT "ID",
		TO_CHAR("TS", 'YYYY-MM-DD HH24:MI:SS.FF6'),
		TO_CHAR("TSTZ" AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.FF6'),
		TO_CHAR("D", 'YYYY-MM-DD'),
		"FLAG" FROM ` + table + ` ORDER BY "ID"`)
	if res.err != nil {
		t.Fatalf("value query: %v", res.err)
	}
	want := [][]string{
		{"1", "2024-03-15 10:30:45.123456", "2024-06-01 16:00:00.500000", "2024-12-31", "1"},
		{"2", "<null>", "<null>", "<null>", "0"},
	}
	if len(res.rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(res.rows), len(want))
	}
	for i := range want {
		for j := range want[i] {
			if res.rows[i][j] != want[i][j] {
				t.Errorf("row %d col %d: got %q, want %q", i, j, res.rows[i][j], want[i][j])
			}
		}
	}
}

// TestIntegration_IngestReplaceOnRetry covers the ORA-00955 scenario: a failed
// run leaves a stub table; a retry with mode=replace must drop and recreate it.
func TestIntegration_IngestReplaceOnRetry(t *testing.T) {
	cnxn, query := openTestConnection(t)
	table := "ADBC_TEST_REPLACE"
	execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE")
	t.Cleanup(func() { execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE") })

	// Simulate the stub left behind by a previous failed run.
	if err := execTestSQL(t, cnxn, `CREATE TABLE `+table+` ("STALE" NUMBER(1))`); err != nil {
		t.Fatalf("create stub: %v", err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	// mode=create against the stub must fail with ORA-00955…
	if _, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeCreate, nil, schema, []arrow.RecordBatch{rec}); err == nil {
		t.Fatal("mode=create over existing table: expected ORA-00955, got success")
	}

	// …and mode=replace must succeed.
	if _, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeReplace, nil, schema, []arrow.RecordBatch{rec}); err != nil {
		t.Fatalf("mode=replace: %v", err)
	}

	res := query(`SELECT COUNT(*) FROM ` + table)
	if res.err != nil || len(res.rows) != 1 || res.rows[0][0] != "2" {
		t.Errorf("post-replace count: rows=%v err=%v, want 2", res.rows, res.err)
	}
}

// TestIntegration_IngestClobColumnsOption covers wide values that only appear
// beyond the auto-scan window: forcing specific columns to CLOB via option.
func TestIntegration_IngestClobColumnsOption(t *testing.T) {
	cnxn, query := openTestConnection(t)
	table := "ADBC_TEST_CLOB_OPT"
	execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE")
	t.Cleanup(func() { execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE") })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Batch 1 has only narrow values; batch 2 goes wide. With the scan window
	// shrunk to 1 byte, auto-sizing sees just batch 1 — the option must save us.
	mk := func(ids []int64, payloads []string) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		b.Field(1).(*array.StringBuilder).AppendValues(payloads, nil)
		return b.NewRecord()
	}
	rec1 := mk([]int64{1}, []string{"tiny"})
	defer rec1.Release()
	rec2 := mk([]int64{2}, []string{strings.Repeat("z", 12000)})
	defer rec2.Release()

	opts := map[string]string{
		OptionIngestClobColumns:   "payload",
		OptionIngestTypeScanLimit: "1",
	}
	affected, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeCreate, opts, schema, []arrow.RecordBatch{rec1, rec2})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if affected != 2 {
		t.Errorf("affected = %d, want 2", affected)
	}

	res := query(`SELECT DBMS_LOB.GETLENGTH("PAYLOAD") FROM ` + table + ` WHERE "ID" = 2`)
	if res.err != nil || len(res.rows) != 1 || res.rows[0][0] != "12000" {
		t.Errorf("payload length: rows=%v err=%v, want 12000", res.rows, res.err)
	}
}

// TestIntegration_IngestMultiBatchScan verifies the buffered-scan pipeline:
// several batches, wide value in the second batch (still inside the scan
// window), everything inserted exactly once in order.
func TestIntegration_IngestMultiBatchScan(t *testing.T) {
	cnxn, query := openTestConnection(t)
	table := "ADBC_TEST_MULTIBATCH"
	execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE")
	t.Cleanup(func() { execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE") })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	var recs []arrow.RecordBatch
	total := 0
	for batch := 0; batch < 4; batch++ {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		ids := make([]int64, 100)
		payloads := make([]string, 100)
		for i := range ids {
			ids[i] = int64(total)
			payloads[i] = fmt.Sprintf("row-%d", total)
			total++
		}
		if batch == 1 {
			payloads[50] = strings.Repeat("w", 9000) // wide, second batch
		}
		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		b.Field(1).(*array.StringBuilder).AppendValues(payloads, nil)
		recs = append(recs, b.NewRecord())
		b.Release()
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	affected, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeCreate, nil, schema, recs)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if affected != int64(total) {
		t.Errorf("affected = %d, want %d", affected, total)
	}

	res := query(`SELECT COUNT(*), COUNT(DISTINCT "ID"), MAX(DBMS_LOB.GETLENGTH("PAYLOAD")) FROM ` + table)
	if res.err != nil {
		t.Fatalf("verify query: %v", res.err)
	}
	if res.rows[0][0] != "400" || res.rows[0][1] != "400" {
		t.Errorf("count/distinct = %v, want 400/400 (no dup or dropped rows)", res.rows[0])
	}
	if res.rows[0][2] != "9000" {
		t.Errorf("max payload = %v, want 9000", res.rows[0][2])
	}
}
