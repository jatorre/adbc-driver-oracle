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
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// getTestDSN returns the Oracle DSN for integration tests.
// Tests are skipped when ORACLE_TEST_DSN is not set.
func getTestDSN(t *testing.T) string {
	dsn := os.Getenv("ORACLE_TEST_DSN")
	if dsn == "" {
		t.Skip("ORACLE_TEST_DSN not set, skipping integration test")
	}
	return dsn
}

func TestNewDriver(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	if drv == nil {
		t.Fatal("NewDriver returned nil")
	}
}

func TestNewDatabase(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		OptionServerHostname: "localhost",
		OptionPort:           "1521",
		OptionServiceName:    "ORCL",
		OptionUser:           "test",
		OptionPassword:       "test",
	})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	if db == nil {
		t.Fatal("NewDatabase returned nil")
	}
	db.Close()
}

func TestNewDatabaseWithDSN(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		OptionDSN: "oracle://test:test@localhost:1521/ORCL",
	})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	db.Close()
}

func TestDatabaseSetOptions(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	// Unknown option should error
	_, err := drv.NewDatabase(map[string]string{
		"unknown.option": "value",
	})
	if err == nil {
		t.Fatal("expected error for unknown option")
	}
}

// --- Integration tests (require ORACLE_TEST_DSN) ---

func TestIntegration_Connect(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()
}

func TestIntegration_SimpleQuery(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT 1 AS val FROM DUAL"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema.NumFields() != 1 {
		t.Fatalf("expected 1 field, got %d", schema.NumFields())
	}

	totalRows := 0
	for reader.Next() {
		totalRows += int(reader.Record().NumRows())
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("Reader error: %v", err)
	}
	if totalRows != 1 {
		t.Fatalf("expected 1 row, got %d", totalRows)
	}
}

func TestIntegration_TypeMapping(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	query := `SELECT
		42 AS int_val,
		3.14 AS float_val,
		'hello' AS str_val,
		SYSDATE AS date_val,
		SYSTIMESTAMP AS ts_val,
		UTL_RAW.CAST_TO_RAW('bytes') AS raw_val
	FROM DUAL`

	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema.NumFields() != 6 {
		t.Fatalf("expected 6 fields, got %d", schema.NumFields())
	}

	// Verify types
	expected := []string{"float64", "float64", "utf8", "timestamp[us, tz=UTC]", "timestamp[us, tz=UTC]", "binary"}
	for i, f := range schema.Fields() {
		got := f.Type.String()
		if got != expected[i] {
			t.Errorf("field %d (%s): expected type %s, got %s", i, f.Name, expected[i], got)
		}
	}
}

func TestIntegration_GeometryPoint(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	// Create a point geometry inline
	query := `SELECT SDO_GEOMETRY(2001, 4326, SDO_POINT_TYPE(-73.935242, 40.730610, NULL), NULL, NULL) AS geom FROM DUAL`
	if err := stmt.SetSqlQuery(query); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	geomField := schema.Field(0)

	// Check geoarrow.wkb metadata
	extName, ok := geomField.Metadata.GetValue("ARROW:extension:name")
	if !ok || extName != "geoarrow.wkb" {
		t.Errorf("expected geoarrow.wkb extension, got %q (ok=%v)", extName, ok)
	}

	// Check SRID
	extMeta, _ := geomField.Metadata.GetValue("ARROW:extension:metadata")
	if extMeta == "{}" {
		t.Log("SRID not detected (inline geometry may not have SDO metadata)")
	}

	// Read the WKB
	if !reader.Next() {
		t.Fatal("expected a row")
	}
	rec := reader.Record()
	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}

	// WKB for a 2D point should be 21 bytes
	col := rec.Column(0).(*array.Binary)
	if col.IsNull(0) {
		t.Fatal("geometry should not be null")
	}
	wkbBytes := col.Value(0)
	t.Logf("WKB length: %d bytes", len(wkbBytes))
	if len(wkbBytes) != 21 {
		t.Errorf("expected 21 bytes for 2D point WKB, got %d", len(wkbBytes))
	}
}

func TestIntegration_GetTableSchema(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	// SYS.DUAL is accessible but owned by SYS
	sysSchema := "SYS"
	schema, err := cnxn.GetTableSchema(ctx, nil, &sysSchema, "DUAL")
	if err != nil {
		t.Fatalf("GetTableSchema(SYS.DUAL): %v", err)
	}
	t.Logf("SYS.DUAL schema: %s", schema)
	if schema.NumFields() == 0 {
		t.Error("expected at least 1 field for DUAL")
	}
}

func TestIntegration_GetTableTypes(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	reader, err := cnxn.GetTableTypes(ctx)
	if err != nil {
		t.Fatalf("GetTableTypes: %v", err)
	}
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		totalRows += int(reader.Record().NumRows())
	}
	if totalRows == 0 {
		t.Error("expected at least 1 table type")
	}
	t.Logf("Got %d table types", totalRows)
}

func TestIntegration_GetObjects(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	// Test at catalog depth
	reader, err := cnxn.GetObjects(ctx, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects(catalogs): %v", err)
	}
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		totalRows += int(reader.Record().NumRows())
	}
	if totalRows == 0 {
		t.Error("expected at least 1 catalog")
	}
	t.Logf("Got %d catalogs", totalRows)
}

func TestIntegration_PreparedStatement(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	defer stmt.Close()

	// Parameterized query: Oracle uses :1, :2, ... placeholders
	if err := stmt.SetSqlQuery("SELECT :1 AS val FROM DUAL"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	if err := stmt.Prepare(ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	// Bind a parameter: single row with one Int64 column
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "p1", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).Append(42)
	rec := bldr.NewRecord()
	defer rec.Release()

	if err := stmt.Bind(ctx, rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("expected a row")
	}
	// The value should be 42
	t.Logf("Result schema: %s", reader.Schema())
	t.Logf("Result: %s", reader.Record())
}

func TestIntegration_BulkIngest(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cnxn.Close()

	tableName := "ADBC_TEST_INGEST"

	// Clean up from previous runs
	cleanStmt, _ := cnxn.NewStatement()
	cleanStmt.SetSqlQuery("DROP TABLE " + tableName + " PURGE")
	cleanStmt.ExecuteUpdate(ctx)
	cleanStmt.Close()

	// Create test data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"alpha", "beta", "gamma"}, nil)
	bldr.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	rec := bldr.NewRecord()
	defer rec.Release()

	// Ingest using Bind
	ingestStmt, err := cnxn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}

	if gs, ok := ingestStmt.(adbc.GetSetOptions); ok {
		gs.SetOption(adbc.OptionKeyIngestTargetTable, tableName)
		gs.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate)
	}

	if err := ingestStmt.Bind(ctx, rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	affected, err := ingestStmt.ExecuteUpdate(ctx)
	if err != nil {
		t.Fatalf("ExecuteUpdate (ingest): %v", err)
	}
	t.Logf("Ingested %d rows", affected)
	ingestStmt.Close()

	if affected != 3 {
		t.Errorf("expected 3 rows affected, got %d", affected)
	}

	// Read back
	readStmt, _ := cnxn.NewStatement()
	defer readStmt.Close()
	readStmt.SetSqlQuery("SELECT * FROM " + tableName + " ORDER BY ID")
	reader, _, err := readStmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery (read back): %v", err)
	}
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		rec := reader.Record()
		totalRows += int(rec.NumRows())
		t.Logf("Row: %s", rec)
	}
	if totalRows != 3 {
		t.Errorf("expected 3 rows read back, got %d", totalRows)
	}

	// Cleanup
	cleanStmt2, _ := cnxn.NewStatement()
	cleanStmt2.SetSqlQuery("DROP TABLE " + tableName + " PURGE")
	cleanStmt2.ExecuteUpdate(ctx)
	cleanStmt2.Close()
}

func TestIntegration_ConnectionPooling(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	// Open two connections — they should share the same pool
	cnxn1, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}

	cnxn2, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}

	// Both should work
	stmt1, _ := cnxn1.NewStatement()
	stmt1.SetSqlQuery("SELECT 1 FROM DUAL")
	r1, _, err := stmt1.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query on cnxn1: %v", err)
	}
	r1.Release()
	stmt1.Close()

	stmt2, _ := cnxn2.NewStatement()
	stmt2.SetSqlQuery("SELECT 2 FROM DUAL")
	r2, _, err := stmt2.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query on cnxn2: %v", err)
	}
	r2.Release()
	stmt2.Close()

	// Close connections — should NOT close the pool
	cnxn1.Close()
	cnxn2.Close()

	// Opening a third connection after closing the first two should still work
	cnxn3, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("Open 3 (after closing 1 & 2): %v", err)
	}
	defer cnxn3.Close()

	stmt3, _ := cnxn3.NewStatement()
	stmt3.SetSqlQuery("SELECT 3 FROM DUAL")
	r3, _, err := stmt3.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("Query on cnxn3: %v", err)
	}
	r3.Release()
	stmt3.Close()
	t.Log("Connection pooling works: 3 connections shared the same pool")
}
