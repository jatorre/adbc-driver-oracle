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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// =============================================================================
// Test data generators
// =============================================================================

// buildScalarTestData creates a record batch with N rows of scalar data.
func buildScalarTestData(alloc memory.Allocator, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	rng := rand.New(rand.NewSource(42))
	categories := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	names := make([]string, 100)
	for i := range names {
		names[i] = fmt.Sprintf("item_%06d", i)
	}

	for i := 0; i < numRows; i++ {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i))
		bldr.Field(1).(*array.StringBuilder).Append(names[i%len(names)])
		bldr.Field(2).(*array.Float64Builder).Append(rng.Float64() * 1000)
		bldr.Field(3).(*array.StringBuilder).Append(categories[rng.Intn(len(categories))])
		bldr.Field(4).(*array.Int64Builder).Append(int64(rng.Intn(10000)))
	}

	return bldr.NewRecord()
}

// makePointWKB creates a WKB Point (21 bytes) from lon/lat.
func makePointWKB(lon, lat float64) []byte {
	buf := make([]byte, 21)
	buf[0] = 1 // little-endian
	binary.LittleEndian.PutUint32(buf[1:5], 1) // wkbPoint
	binary.LittleEndian.PutUint64(buf[5:13], math.Float64bits(lon))
	binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(lat))
	return buf
}

// makePolygonWKB creates a WKB Polygon (small rectangle) simulating a building footprint.
func makePolygonWKB(lon, lat float64) []byte {
	d := 0.0001
	ring := [][2]float64{
		{lon - d, lat - d}, {lon + d, lat - d},
		{lon + d, lat + d}, {lon - d, lat + d},
		{lon - d, lat - d},
	}
	size := 1 + 4 + 4 + 4 + len(ring)*16
	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], 3)
	binary.LittleEndian.PutUint32(buf[5:9], 1)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(ring)))
	off := 13
	for _, pt := range ring {
		binary.LittleEndian.PutUint64(buf[off:off+8], math.Float64bits(pt[0]))
		binary.LittleEndian.PutUint64(buf[off+8:off+16], math.Float64bits(pt[1]))
		off += 16
	}
	return buf
}

// geomSchema returns an Arrow schema with id, name, and a geoarrow.wkb geometry column.
func geomSchema() *arrow.Schema {
	geomMD := arrow.MetadataFrom(map[string]string{
		"ARROW:extension:name":     "geoarrow.wkb",
		"ARROW:extension:metadata": `{"columns":{"geometry":{"encoding":"wkb","crs":{"id":{"authority":"EPSG","code":4326}}}}}`,
	})
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geomMD},
	}, nil)
}

// buildGeomBatch creates a single batch of geometry rows.
func buildGeomBatch(alloc memory.Allocator, schema *arrow.Schema, startID, batchSize int, usePolygons bool, rng *rand.Rand) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()
	names := []string{"building_a", "building_b", "park", "shop", "house", "office", "school", "church"}
	for i := 0; i < batchSize; i++ {
		lon := 12.1 + rng.Float64()*6.8
		lat := 48.5 + rng.Float64()*2.6
		bldr.Field(0).(*array.Int64Builder).Append(int64(startID + i))
		bldr.Field(1).(*array.StringBuilder).Append(names[rng.Intn(len(names))])
		if usePolygons {
			bldr.Field(2).(*array.BinaryBuilder).Append(makePolygonWKB(lon, lat))
		} else {
			bldr.Field(2).(*array.BinaryBuilder).Append(makePointWKB(lon, lat))
		}
	}
	return bldr.NewRecord()
}

// geomRecordStream implements array.RecordReader for streaming synthetic geometry batches.
type geomRecordStream struct {
	alloc       memory.Allocator
	schema      *arrow.Schema
	totalRows   int
	batchSize   int
	usePolygons bool
	rng         *rand.Rand
	current     arrow.Record
	nextID      int
	refCount    int64
}

func newGeomStream(alloc memory.Allocator, totalRows, batchSize int, usePolygons bool) *geomRecordStream {
	return &geomRecordStream{
		alloc: alloc, schema: geomSchema(), totalRows: totalRows,
		batchSize: batchSize, usePolygons: usePolygons,
		rng: rand.New(rand.NewSource(42)), refCount: 1,
	}
}

func (s *geomRecordStream) Schema() *arrow.Schema { return s.schema }
func (s *geomRecordStream) Next() bool {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
	if s.nextID >= s.totalRows {
		return false
	}
	bs := s.batchSize
	if remaining := s.totalRows - s.nextID; remaining < bs {
		bs = remaining
	}
	s.current = buildGeomBatch(s.alloc, s.schema, s.nextID, bs, s.usePolygons, s.rng)
	s.nextID += bs
	return true
}
func (s *geomRecordStream) Record() arrow.Record          { return s.current }
func (s *geomRecordStream) RecordBatch() arrow.RecordBatch { return s.current }
func (s *geomRecordStream) Err() error                     { return nil }
func (s *geomRecordStream) Retain()                        { s.refCount++ }
func (s *geomRecordStream) Release() {
	s.refCount--
	if s.refCount <= 0 && s.current != nil {
		s.current.Release()
		s.current = nil
	}
}

// =============================================================================
// Real buildings from Czech Republic OSM (parquet)
// =============================================================================

type parquetBuildingStream struct {
	pf       *file.Reader
	rr       pqarrow.RecordReader
	schema   *arrow.Schema
	refCount int64
	current  arrow.Record
	maxRows  int64
	emitted  int64
}

func newParquetBuildingStream(alloc memory.Allocator, parquetPath string) (*parquetBuildingStream, error) {
	f, err := os.Open(parquetPath)
	if err != nil {
		return nil, err
	}
	pf, err := file.NewParquetReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 10000}, alloc)
	if err != nil {
		pf.Close()
		return nil, err
	}
	rr, err := fr.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		pf.Close()
		return nil, err
	}

	geomMD := arrow.MetadataFrom(map[string]string{
		"ARROW:extension:name":     "geoarrow.wkb",
		"ARROW:extension:metadata": `{"columns":{"geometry":{"encoding":"wkb","crs":{"id":{"authority":"EPSG","code":4326}}}}}`,
	})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: geomMD},
	}, nil)

	return &parquetBuildingStream{pf: pf, rr: rr, schema: schema, refCount: 1}, nil
}

func (s *parquetBuildingStream) Schema() *arrow.Schema { return s.schema }
func (s *parquetBuildingStream) Next() bool {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
	if s.maxRows > 0 && s.emitted >= s.maxRows {
		return false
	}
	if !s.rr.Next() {
		return false
	}
	src := s.rr.Record()
	numRows := int(src.NumRows())
	if s.maxRows > 0 {
		if remaining := int(s.maxRows - s.emitted); numRows > remaining {
			numRows = remaining
		}
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, s.schema)
	defer bldr.Release()

	osmCol := src.Column(0).(*array.String)
	nameCol := src.Column(1).(*array.String)
	wkbCol := src.Column(2).(*array.Binary)

	for i := 0; i < numRows; i++ {
		var id int64
		if !osmCol.IsNull(i) {
			for _, c := range osmCol.Value(i) {
				id = id*31 + int64(c)
			}
		}
		bldr.Field(0).(*array.Int64Builder).Append(id)
		if nameCol.IsNull(i) {
			bldr.Field(1).(*array.StringBuilder).AppendNull()
		} else {
			bldr.Field(1).(*array.StringBuilder).Append(nameCol.Value(i))
		}
		if wkbCol.IsNull(i) {
			bldr.Field(2).(*array.BinaryBuilder).AppendNull()
		} else {
			bldr.Field(2).(*array.BinaryBuilder).Append(wkbCol.Value(i))
		}
	}
	s.current = bldr.NewRecord()
	s.emitted += int64(s.current.NumRows())
	return true
}
func (s *parquetBuildingStream) Record() arrow.Record          { return s.current }
func (s *parquetBuildingStream) RecordBatch() arrow.RecordBatch { return s.current }
func (s *parquetBuildingStream) Err() error                     { return s.rr.Err() }
func (s *parquetBuildingStream) Retain()                        { s.refCount++ }
func (s *parquetBuildingStream) Release() {
	s.refCount--
	if s.refCount <= 0 {
		if s.current != nil {
			s.current.Release()
		}
		if s.rr != nil {
			s.rr.Release()
		}
		if s.pf != nil {
			s.pf.Close()
		}
	}
}

// =============================================================================
// Helpers
// =============================================================================

func openADBC(t *testing.T, dsn string) (adbc.Database, adbc.Connection) {
	t.Helper()
	ctx := context.Background()
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{OptionDSN: dsn})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	cnxn, err := db.Open(ctx)
	if err != nil {
		db.Close()
		t.Fatalf("Open: %v", err)
	}
	return db, cnxn
}

func execSQL(t *testing.T, cnxn adbc.Connection, sql string) {
	t.Helper()
	ctx := context.Background()
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()
	stmt.SetSqlQuery(sql)
	stmt.ExecuteUpdate(ctx)
}

func verifyCount(t *testing.T, cnxn adbc.Connection, tableName string) {
	t.Helper()
	ctx := context.Background()
	stmt, _ := cnxn.NewStatement()
	defer stmt.Close()
	stmt.SetSqlQuery("SELECT COUNT(*) FROM " + tableName)
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	defer reader.Release()
	if reader.Next() {
		t.Logf("Verified row count: %s", reader.Record())
	}
}

func getRowCount(t *testing.T) int {
	numRows := 50000
	if n := os.Getenv("BENCH_ROWS"); n != "" {
		fmt.Sscanf(n, "%d", &numRows)
	}
	return numRows
}

// =============================================================================
// Benchmarks
// =============================================================================

// TestBenchmark_ScalarInsert benchmarks batch INSERT with scalar data (no geometry).
func TestBenchmark_ScalarInsert(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()
	numRows := getRowCount(t)

	rec := buildScalarTestData(memory.DefaultAllocator, numRows)
	defer rec.Release()

	db, cnxn := openADBC(t, dsn)
	defer db.Close()
	defer cnxn.Close()

	tableName := "BENCH_SCALAR"
	execSQL(t, cnxn, "DROP TABLE "+tableName+" PURGE")

	ingestStmt, _ := cnxn.NewStatement()
	if gs, ok := ingestStmt.(adbc.GetSetOptions); ok {
		gs.SetOption(adbc.OptionKeyIngestTargetTable, tableName)
		gs.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate)
	}
	ingestStmt.Bind(ctx, rec)

	t.Logf("=== Scalar INSERT: %d rows, 5 columns ===", numRows)
	start := time.Now()
	affected, err := ingestStmt.ExecuteUpdate(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	ingestStmt.Close()

	t.Logf("Result: %d rows in %v (%.0f rows/sec)", affected, elapsed, float64(affected)/elapsed.Seconds())
	verifyCount(t, cnxn, tableName)
	execSQL(t, cnxn, "DROP TABLE "+tableName+" PURGE")
}

// TestBenchmark_GeomPoints benchmarks WKB point → SDO_GEOMETRY ingest.
func TestBenchmark_GeomPoints(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()
	numRows := getRowCount(t)

	db, cnxn := openADBC(t, dsn)
	defer db.Close()
	defer cnxn.Close()

	tableName := "BENCH_GEOM_POINTS"
	execSQL(t, cnxn, "DROP TABLE "+tableName+" PURGE")

	stream := newGeomStream(memory.DefaultAllocator, numRows, 50000, false)
	ingestStmt, _ := cnxn.NewStatement()
	if gs, ok := ingestStmt.(adbc.GetSetOptions); ok {
		gs.SetOption(adbc.OptionKeyIngestTargetTable, tableName)
		gs.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate)
	}
	ingestStmt.BindStream(ctx, stream)

	t.Logf("=== WKB Points → SDO_GEOMETRY: %d rows ===", numRows)
	start := time.Now()
	affected, err := ingestStmt.ExecuteUpdate(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	ingestStmt.Close()

	t.Logf("Result: %d rows in %v (%.0f rows/sec)", affected, elapsed, float64(affected)/elapsed.Seconds())
	verifyCount(t, cnxn, tableName)
	execSQL(t, cnxn, "DROP TABLE "+tableName+" PURGE")
}

// TestBenchmark_GeomPolygons benchmarks WKB polygon → SDO_GEOMETRY ingest.
func TestBenchmark_GeomPolygons(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()
	numRows := getRowCount(t)

	db, cnxn := openADBC(t, dsn)
	defer db.Close()
	defer cnxn.Close()

	tableName := "BENCH_GEOM_POLYGONS"
	execSQL(t, cnxn, "DROP TABLE "+tableName+" PURGE")

	stream := newGeomStream(memory.DefaultAllocator, numRows, 50000, true)
	ingestStmt, _ := cnxn.NewStatement()
	if gs, ok := ingestStmt.(adbc.GetSetOptions); ok {
		gs.SetOption(adbc.OptionKeyIngestTargetTable, tableName)
		gs.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate)
	}
	ingestStmt.BindStream(ctx, stream)

	t.Logf("=== WKB Polygons → SDO_GEOMETRY: %d rows ===", numRows)
	start := time.Now()
	affected, err := ingestStmt.ExecuteUpdate(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	ingestStmt.Close()

	t.Logf("Result: %d rows in %v (%.0f rows/sec)", affected, elapsed, float64(affected)/elapsed.Seconds())
	verifyCount(t, cnxn, tableName)
	execSQL(t, cnxn, "DROP TABLE "+tableName+" PURGE")
}

// TestBenchmark_RealBuildings benchmarks with real Czech Republic building polygons.
// Requires /tmp/czech_buildings.parquet (generate with DuckDB from Geofabrik shapefiles).
// Set BENCH_ROWS to limit the number of rows (default: all ~5M).
func TestBenchmark_RealBuildings(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	parquetPath := "/tmp/czech_buildings.parquet"
	if _, err := os.Stat(parquetPath); err != nil {
		t.Skipf("Parquet file not found: %s (generate with DuckDB from Geofabrik shapefile)", parquetPath)
	}

	numRows := getRowCount(t)
	db, cnxn := openADBC(t, dsn)
	defer db.Close()
	defer cnxn.Close()

	tableName := "BENCH_REAL_BUILDINGS"
	execSQL(t, cnxn, "DROP TABLE "+tableName+" PURGE")

	stream, err := newParquetBuildingStream(memory.DefaultAllocator, parquetPath)
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	stream.maxRows = int64(numRows)

	totalRows := int64(numRows)
	if stream.pf.NumRows() < totalRows {
		totalRows = stream.pf.NumRows()
	}

	ingestStmt, _ := cnxn.NewStatement()
	if gs, ok := ingestStmt.(adbc.GetSetOptions); ok {
		gs.SetOption(adbc.OptionKeyIngestTargetTable, tableName)
		gs.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace)
	}
	ingestStmt.BindStream(ctx, stream)

	t.Logf("=== Real Buildings → SDO_GEOMETRY: %d rows (avg 7.6 vertices) ===", totalRows)
	start := time.Now()
	affected, err := ingestStmt.ExecuteUpdate(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	ingestStmt.Close()

	t.Logf("Result: %d rows in %v (%.0f rows/sec)", affected, elapsed, float64(affected)/elapsed.Seconds())
	verifyCount(t, cnxn, tableName)
	execSQL(t, cnxn, "DROP TABLE "+tableName+" PURGE")
}

// TestBenchmark_All runs all benchmarks for a complete comparison.
func TestBenchmark_All(t *testing.T) {
	_ = getTestDSN(t)
	t.Run("Scalar", TestBenchmark_ScalarInsert)
	t.Run("Points", TestBenchmark_GeomPoints)
	t.Run("Polygons", TestBenchmark_GeomPolygons)
	t.Run("RealBuildings", TestBenchmark_RealBuildings)
}
