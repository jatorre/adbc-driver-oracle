package oracle

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
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
