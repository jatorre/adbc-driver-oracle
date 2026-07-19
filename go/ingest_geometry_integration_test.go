// Copyright 2026 CARTO
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
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// makeRingCoords returns a closed regular-polygon ring with n distinct
// vertices centered on (cx, cy) — n+1 coordinate pairs, last == first.
func makeRingCoords(n int, cx, cy, radius float64) [][2]float64 {
	ring := make([][2]float64, 0, n+1)
	for i := 0; i < n; i++ {
		a := 2 * math.Pi * float64(i) / float64(n)
		ring = append(ring, [2]float64{cx + radius*math.Cos(a), cy + radius*math.Sin(a)})
	}
	ring = append(ring, ring[0])
	return ring
}

// makeTestPolygonWKB builds a little-endian 2D WKB polygon from rings.
func makeTestPolygonWKB(rings ...[][2]float64) []byte {
	size := 1 + 4 + 4
	for _, r := range rings {
		size += 4 + len(r)*16
	}
	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbPolygon)
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(rings)))
	off := 9
	for _, r := range rings {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(r)))
		off += 4
		for _, pt := range r {
			binary.LittleEndian.PutUint64(buf[off:off+8], math.Float64bits(pt[0]))
			binary.LittleEndian.PutUint64(buf[off+8:off+16], math.Float64bits(pt[1]))
			off += 16
		}
	}
	return buf
}

// makeLineCoords returns an open n-point line starting at (x0, y0).
func makeLineCoords(n int, x0, y0 float64) [][2]float64 {
	pts := make([][2]float64, n)
	for i := range pts {
		pts[i] = [2]float64{x0 + float64(i), y0 + float64(i%7)}
	}
	return pts
}

// makeTestMultiLineStringWKB builds a little-endian 2D WKB multi-linestring.
func makeTestMultiLineStringWKB(lines ...[][2]float64) []byte {
	size := 1 + 4 + 4
	for _, l := range lines {
		size += 1 + 4 + 4 + len(l)*16
	}
	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbMultiLineString)
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(lines)))
	off := 9
	for _, l := range lines {
		buf[off] = 1
		binary.LittleEndian.PutUint32(buf[off+1:off+5], wkbLineString)
		binary.LittleEndian.PutUint32(buf[off+5:off+9], uint32(len(l)))
		off += 9
		for _, pt := range l {
			binary.LittleEndian.PutUint64(buf[off:off+8], math.Float64bits(pt[0]))
			binary.LittleEndian.PutUint64(buf[off+8:off+16], math.Float64bits(pt[1]))
			off += 16
		}
	}
	return buf
}

// TestIntegration_IngestGeometryVarraySizes reproduces the sc-558619 corruption:
// SDO_ORDINATES varrays whose Oracle NUMBER encoding exceeds ~245 bytes land
// EMPTY on the server (rows fail SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT with
// ORA-13031), while smaller rings round-trip fine. Cases bracket the boundary
// and cover the polygon-with-hole shape from the failing DO tables.
func TestIntegration_IngestGeometryVarraySizes(t *testing.T) {
	cnxn, query := openTestConnection(t)
	table := "ADBC_TEST_GEOM_VARRAY"
	execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE")
	t.Cleanup(func() { execTestSQL(t, cnxn, "DROP TABLE "+table+" PURGE") })

	type geomCase struct {
		id       int64
		name     string
		wkb      []byte
		wantOrds int64 // 2 * total coordinate pairs across rings
	}

	ringSizes := []int{4, 12, 14, 30, 100} // distinct vertices per ring
	cases := make([]geomCase, 0, len(ringSizes)+1)
	for i, n := range ringSizes {
		ring := makeRingCoords(n, 10, 10, 1)
		cases = append(cases, geomCase{
			id:       int64(i + 1),
			name:     fmt.Sprintf("ring%dpts", n),
			wkb:      makeTestPolygonWKB(ring),
			wantOrds: int64(2 * (n + 1)),
		})
	}
	outer := makeRingCoords(40, 0, 0, 10)
	hole := makeRingCoords(8, 0, 0, 2)
	cases = append(cases, geomCase{
		id:       int64(len(ringSizes) + 1),
		name:     "polygonWithHole",
		wkb:      makeTestPolygonWKB(outer, hole),
		wantOrds: int64(2 * (len(outer) + len(hole))),
	})
	cases = append(cases, geomCase{
		id:       int64(len(ringSizes) + 2),
		name:     "multiLineString60pts",
		wkb:      makeTestMultiLineStringWKB(makeLineCoords(60, 0, 0), makeLineCoords(5, 50, 50)),
		wantOrds: int64(2 * (60 + 5)),
	})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, c := range cases {
		b.Field(0).(*array.Int64Builder).Append(c.id)
		b.Field(1).(*array.BinaryBuilder).Append(c.wkb)
	}
	rec := b.NewRecord()
	defer rec.Release()

	affected, err := ingestStream(t, cnxn, table, adbc.OptionValueIngestModeCreate, nil, schema, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if affected != int64(len(cases)) {
		t.Errorf("affected = %d, want %d", affected, len(cases))
	}

	for _, c := range cases {
		res := query(fmt.Sprintf(
			`SELECT (SELECT COUNT(*) FROM TABLE(g.GEOM.SDO_ORDINATES)),
			        SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(g.GEOM, 0.05)
			 FROM %s g WHERE g.ID = %d`, table, c.id))
		if res.err != nil {
			t.Fatalf("%s: verify query: %v", c.name, res.err)
		}
		if len(res.rows) != 1 {
			t.Fatalf("%s: expected 1 row, got %d", c.name, len(res.rows))
		}
		gotOrds, valid := res.rows[0][0], res.rows[0][1]
		if gotOrds != fmt.Sprint(c.wantOrds) {
			t.Errorf("%s: SDO_ORDINATES count = %s, want %d", c.name, gotOrds, c.wantOrds)
		}
		if valid != "TRUE" {
			t.Errorf("%s: VALIDATE_GEOMETRY_WITH_CONTEXT = %q, want TRUE", c.name, valid)
		}
	}
}
