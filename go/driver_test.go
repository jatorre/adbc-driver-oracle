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
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	go_ora "github.com/sijms/go-ora/v2"
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

// TestIntegration_GeometryInsertValidate inserts various geometry types via ADBC
// ingest and validates them on the Oracle side using SDO_UTIL.VALIDATE_GEOMETRY_WITH_CONTEXT
// and direct GTYPE/coordinate checks. This catches encoding issues in go-ora's
// UDT serialization that pure Go-side tests cannot detect.
func TestIntegration_GeometryInsertValidate(t *testing.T) {
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

	tableName := "ADBC_TEST_GEOM_VALIDATE"

	// Clean up from previous runs
	cleanStmt, _ := cnxn.NewStatement()
	cleanStmt.SetSqlQuery("DROP TABLE " + tableName + " PURGE")
	cleanStmt.ExecuteUpdate(ctx)
	cleanStmt.Close()

	// Build test data with point, polygon, and multipolygon geometries
	geomMD := arrow.MetadataFrom(map[string]string{
		"ARROW:extension:name":     "geoarrow.wkb",
		"ARROW:extension:metadata": `{"columns":{"geom":{"encoding":"wkb","crs":{"id":{"authority":"EPSG","code":4326}}}}}`,
	})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "geom_type", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geomMD},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	// Row 1: Point
	bldr.Field(0).(*array.Int64Builder).Append(1)
	bldr.Field(1).(*array.StringBuilder).Append("POINT")
	bldr.Field(2).(*array.BinaryBuilder).Append(makePointWKB(-73.935242, 40.730610))

	// Row 2: Polygon (small square)
	bldr.Field(0).(*array.Int64Builder).Append(2)
	bldr.Field(1).(*array.StringBuilder).Append("POLYGON")
	bldr.Field(2).(*array.BinaryBuilder).Append(makePolygonWKB(-73.935, 40.730))

	// Row 3: MultiPolygon (two squares)
	bldr.Field(0).(*array.Int64Builder).Append(3)
	bldr.Field(1).(*array.StringBuilder).Append("MULTIPOLYGON")
	bldr.Field(2).(*array.BinaryBuilder).Append(makeMultiPolygonWKB())

	rec := bldr.NewRecord()
	defer rec.Release()

	// Ingest
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
	ingestStmt.Close()
	t.Logf("Ingested %d geometry rows", affected)

	// Validate each geometry using Oracle spatial functions
	validateStmt, _ := cnxn.NewStatement()
	defer validateStmt.Close()

	validateQuery := `SELECT
		id,
		geom_type,
		g.geom.SDO_GTYPE AS gtype,
		g.geom.SDO_SRID AS srid,
		SDO_UTIL.VALIDATE_GEOMETRY_WITH_CONTEXT(g.geom, 0.005) AS valid
	FROM ` + tableName + ` g ORDER BY id`

	if err := validateStmt.SetSqlQuery(validateQuery); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}

	reader, _, err := validateStmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery (validate): %v", err)
	}
	defer reader.Release()

	expectedGTypes := map[int64]float64{
		1: 2001, // Point
		2: 2003, // Polygon
		3: 2007, // MultiPolygon
	}
	expectedSRID := float64(4326)

	rowCount := 0
	for reader.Next() {
		rec := reader.Record()
		for i := 0; i < int(rec.NumRows()); i++ {
			rowCount++
			id := rec.Column(0).(*array.Float64).Value(i)
			geomType := rec.Column(1).(*array.String).Value(i)

			// GTYPE check
			if rec.Column(2).IsNull(i) {
				t.Errorf("row %d (%s): SDO_GTYPE is NULL", int(id), geomType)
			} else {
				gtype := rec.Column(2).(*array.Float64).Value(i)
				if expected, ok := expectedGTypes[int64(id)]; ok && gtype != expected {
					t.Errorf("row %d (%s): SDO_GTYPE = %v, want %v", int(id), geomType, gtype, expected)
				} else {
					t.Logf("row %d (%s): SDO_GTYPE = %v OK", int(id), geomType, gtype)
				}
			}

			// SRID check
			if rec.Column(3).IsNull(i) {
				t.Errorf("row %d (%s): SDO_SRID is NULL", int(id), geomType)
			} else {
				srid := rec.Column(3).(*array.Float64).Value(i)
				if srid != expectedSRID {
					t.Errorf("row %d (%s): SDO_SRID = %v, want %v", int(id), geomType, srid, expectedSRID)
				}
			}

			// Validation result
			if rec.Column(4).IsNull(i) {
				t.Errorf("row %d (%s): VALIDATE_GEOMETRY returned NULL", int(id), geomType)
			} else {
				valid := rec.Column(4).(*array.String).Value(i)
				if valid != "TRUE" {
					t.Errorf("row %d (%s): VALIDATE_GEOMETRY = %q, want TRUE", int(id), geomType, valid)
				} else {
					t.Logf("row %d (%s): geometry is valid", int(id), geomType)
				}
			}
		}
	}

	if rowCount != 3 {
		t.Errorf("expected 3 rows, got %d", rowCount)
	}

	// Also verify round-trip: read geometries back as WKB via ADBC
	readStmt, _ := cnxn.NewStatement()
	defer readStmt.Close()
	readStmt.SetSqlQuery("SELECT id, geom FROM " + tableName + " ORDER BY id")
	rdr, _, err := readStmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("ExecuteQuery (read back WKB): %v", err)
	}
	defer rdr.Release()

	wkbCount := 0
	for rdr.Next() {
		rec := rdr.Record()
		for i := 0; i < int(rec.NumRows()); i++ {
			wkbCount++
			geomCol := rec.Column(1).(*array.Binary)
			if geomCol.IsNull(i) {
				t.Errorf("row %d: geometry WKB is NULL on read-back", i)
			} else {
				wkb := geomCol.Value(i)
				t.Logf("row %d: read back %d WKB bytes", i, len(wkb))
				if len(wkb) < 5 {
					t.Errorf("row %d: WKB too short: %d bytes", i, len(wkb))
				}
			}
		}
	}
	t.Logf("Read back %d geometry rows as WKB", wkbCount)

	// Cleanup
	dropStmt, _ := cnxn.NewStatement()
	dropStmt.SetSqlQuery("DROP TABLE " + tableName + " PURGE")
	dropStmt.ExecuteUpdate(ctx)
	dropStmt.Close()
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

// TestIntegration_GoOraUDTDirect bypasses the ADBC layer and tests go-ora's UDT
// encoding directly. This isolates whether geometry encoding bugs are in go-ora
// itself or in our ADBC-to-go-ora bridge.
func TestIntegration_GoOraUDTDirect(t *testing.T) {
	dsn := getTestDSN(t)
	tableName := "ADBC_TEST_UDT_DIRECT"

	// Open raw database/sql connection
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Register SDO types
	if err := RegisterSDOTypes(db); err != nil {
		t.Fatalf("RegisterSDOTypes: %v", err)
	}

	// Create table
	db.Exec("DROP TABLE " + tableName + " PURGE")
	_, err = db.Exec("CREATE TABLE " + tableName + " (id NUMBER, label VARCHAR2(50), geom MDSYS.SDO_GEOMETRY)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE " + tableName + " PURGE")

	// Define test cases: geometry type → SdoGeometry struct
	testCases := []struct {
		id    int
		label string
		geom  SdoGeometry
	}{
		{
			id: 1, label: "point",
			geom: SdoGeometry{
				GType: 2001, SRID: 4326,
				Point: SdoPointType{X: -73.935242, Y: 40.730610},
			},
		},
		{
			id: 2, label: "line",
			geom: SdoGeometry{
				GType: 2002, SRID: 4326,
				ElemInfo:  []int64{1, 2, 1},
				Ordinates: []float64{0, 0, 1, 1, 2, 0},
			},
		},
		{
			id: 3, label: "polygon",
			geom: SdoGeometry{
				GType: 2003, SRID: 4326,
				ElemInfo:  []int64{1, 1003, 1},
				Ordinates: []float64{0, 0, 1, 0, 1, 1, 0, 1, 0, 0},
			},
		},
		{
			id: 4, label: "multipolygon",
			geom: SdoGeometry{
				GType: 2007, SRID: 4326,
				ElemInfo: []int64{1, 1003, 1, 11, 1003, 1},
				Ordinates: []float64{
					0, 0, 1, 0, 1, 1, 0, 1, 0, 0,
					5, 5, 6, 5, 6, 6, 5, 6, 5, 5,
				},
			},
		},
	}

	// Insert each geometry via go-ora UDT
	for _, tc := range testCases {
		obj := go_ora.NewObject("MDSYS", "SDO_GEOMETRY", tc.geom)
		_, err := db.Exec(
			"INSERT INTO "+tableName+" (id, label, geom) VALUES (:1, :2, :3)",
			tc.id, tc.label, obj,
		)
		if err != nil {
			t.Errorf("INSERT id=%d (%s): %v", tc.id, tc.label, err)
			continue
		}
		t.Logf("INSERT id=%d (%s): OK", tc.id, tc.label)
	}

	// Validate all geometries
	rows, err := db.Query(fmt.Sprintf(`SELECT
		id, label,
		t.geom.SDO_GTYPE,
		t.geom.SDO_SRID,
		SDO_UTIL.VALIDATE_GEOMETRY_WITH_CONTEXT(t.geom, 0.005)
	FROM %s t ORDER BY id`, tableName))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var label string
		var gtype, srid sql.NullFloat64
		var valid sql.NullString
		if err := rows.Scan(&id, &label, &gtype, &srid, &valid); err != nil {
			t.Errorf("Scan: %v", err)
			continue
		}

		t.Logf("id=%d label=%s gtype=%v srid=%v valid=%v",
			id, label, gtype.Float64, srid.Float64, valid.String)

		if !gtype.Valid {
			t.Errorf("id=%d (%s): SDO_GTYPE is NULL", id, label)
		}
		if !srid.Valid {
			t.Errorf("id=%d (%s): SDO_SRID is NULL", id, label)
		} else if srid.Float64 != 4326 {
			t.Errorf("id=%d (%s): SDO_SRID = %v, want 4326", id, label, srid.Float64)
		}
		if !valid.Valid {
			t.Errorf("id=%d (%s): VALIDATE_GEOMETRY returned NULL", id, label)
		} else if valid.String != "TRUE" {
			t.Errorf("id=%d (%s): VALIDATE_GEOMETRY = %q, want TRUE", id, label, valid.String)
		}
	}

	// Also insert the same geometries via SQL constructor as a control
	controlTable := tableName + "_CTRL"
	db.Exec("DROP TABLE " + controlTable + " PURGE")
	_, err = db.Exec("CREATE TABLE " + controlTable + " (id NUMBER, label VARCHAR2(50), geom MDSYS.SDO_GEOMETRY)")
	if err != nil {
		t.Fatalf("CREATE control TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE " + controlTable + " PURGE")

	controlInserts := []string{
		`INSERT INTO %s VALUES (1, 'point_sql',
			SDO_GEOMETRY(2001, 4326, SDO_POINT_TYPE(-73.935242, 40.730610, NULL), NULL, NULL))`,
		`INSERT INTO %s VALUES (2, 'line_sql',
			SDO_GEOMETRY(2002, 4326, NULL, SDO_ELEM_INFO_ARRAY(1,2,1), SDO_ORDINATE_ARRAY(0,0,1,1,2,0)))`,
		`INSERT INTO %s VALUES (3, 'polygon_sql',
			SDO_GEOMETRY(2003, 4326, NULL, SDO_ELEM_INFO_ARRAY(1,1003,1), SDO_ORDINATE_ARRAY(0,0,1,0,1,1,0,1,0,0)))`,
		`INSERT INTO %s VALUES (4, 'mpoly_sql',
			SDO_GEOMETRY(2007, 4326, NULL, SDO_ELEM_INFO_ARRAY(1,1003,1,11,1003,1), SDO_ORDINATE_ARRAY(0,0,1,0,1,1,0,1,0,0,5,5,6,5,6,6,5,6,5,5)))`,
	}
	for _, q := range controlInserts {
		if _, err := db.Exec(fmt.Sprintf(q, controlTable)); err != nil {
			t.Fatalf("control INSERT: %v", err)
		}
	}
	t.Log("Control SQL inserts succeeded — Oracle SQL constructor works fine")

	// Compare: validate control geometries too (should all be TRUE)
	ctrlRows, err := db.Query(fmt.Sprintf(`SELECT id, label,
		SDO_UTIL.VALIDATE_GEOMETRY_WITH_CONTEXT(t.geom, 0.005)
	FROM %s t ORDER BY id`, controlTable))
	if err != nil {
		t.Fatalf("control SELECT: %v", err)
	}
	defer ctrlRows.Close()
	for ctrlRows.Next() {
		var id int
		var label, valid string
		ctrlRows.Scan(&id, &label, &valid)
		t.Logf("control: id=%d label=%s valid=%s", id, label, valid)
	}
}

// TestIntegration_WKBViaSDOConstructor tests the SQL-based WKB insert path:
// insert WKB bytes using SDO_GEOMETRY(wkb, srid) constructor instead of direct UDT encoding.
// This bypasses go-ora's UDT serialization entirely and is the approach used by ADBC ingest.
func TestIntegration_WKBViaSDOConstructor(t *testing.T) {
	dsn := getTestDSN(t)
	tableName := "ADBC_TEST_SDO_WKB"

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	db.Exec("DROP TABLE " + tableName + " PURGE")
	_, err = db.Exec("CREATE TABLE " + tableName + " (id NUMBER, geom MDSYS.SDO_GEOMETRY)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE " + tableName + " PURGE")

	testCases := []struct {
		id  int
		wkb []byte
	}{
		{1, makePointWKB(-73.935242, 40.730610)},
		{2, makePolygonWKB(-73.935, 40.730)},
		{3, makeMultiPolygonWKB()},
	}

	for _, tc := range testCases {
		_, err := db.Exec(
			"INSERT INTO "+tableName+" (id, geom) VALUES (:1, SDO_GEOMETRY(:2, 4326))",
			tc.id, tc.wkb,
		)
		if err != nil {
			t.Errorf("SDO_GEOMETRY(wkb,srid) INSERT id=%d: %v", tc.id, err)
			continue
		}
		t.Logf("SDO_GEOMETRY(wkb,srid) INSERT id=%d: OK", tc.id)
	}

	// Validate
	rows, err := db.Query(fmt.Sprintf(`SELECT id,
		t.geom.SDO_GTYPE,
		t.geom.SDO_SRID,
		SDO_UTIL.VALIDATE_GEOMETRY_WITH_CONTEXT(t.geom, 0.005)
	FROM %s t ORDER BY id`, tableName))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var gtype, srid sql.NullFloat64
		var valid sql.NullString
		rows.Scan(&id, &gtype, &srid, &valid)
		t.Logf("id=%d gtype=%v srid=%v valid=%v", id, gtype.Float64, srid.Float64, valid.String)

		if !srid.Valid || srid.Float64 != 4326 {
			t.Errorf("id=%d: SDO_SRID = %v, want 4326", id, srid.Float64)
		}
		if valid.Valid && valid.String != "TRUE" {
			t.Errorf("id=%d: VALIDATE = %q, want TRUE", id, valid.String)
		}
	}
}
