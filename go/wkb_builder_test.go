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
	"encoding/binary"
	"math"
	"testing"
)

func TestSdoToWKB_Point2D(t *testing.T) {
	geom := &SdoGeometry{
		GType: 2001,
		SRID:  4326,
		Point: SdoPointType{X: -73.935242, Y: 40.730610, Z: 0},
	}

	wkb, err := SdoToWKB(geom)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// WKB Point: 1 + 4 + 16 = 21 bytes
	if len(wkb) != 21 {
		t.Fatalf("expected 21 bytes, got %d", len(wkb))
	}

	if wkb[0] != 1 {
		t.Fatalf("expected little-endian byte order, got %d", wkb[0])
	}

	wkbType := binary.LittleEndian.Uint32(wkb[1:5])
	if wkbType != 1 {
		t.Fatalf("expected WKB type 1 (Point), got %d", wkbType)
	}

	x := math.Float64frombits(binary.LittleEndian.Uint64(wkb[5:13]))
	y := math.Float64frombits(binary.LittleEndian.Uint64(wkb[13:21]))

	if math.Abs(x-(-73.935242)) > 1e-6 {
		t.Fatalf("expected X=-73.935242, got %f", x)
	}
	if math.Abs(y-40.730610) > 1e-6 {
		t.Fatalf("expected Y=40.730610, got %f", y)
	}
}

func TestSdoToWKB_Point3D(t *testing.T) {
	geom := &SdoGeometry{
		GType: 3001,
		SRID:  4326,
		Point: SdoPointType{X: 1.0, Y: 2.0, Z: 3.0},
	}

	wkb, err := SdoToWKB(geom)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(wkb) != 29 { // 1 + 4 + 24
		t.Fatalf("expected 29 bytes, got %d", len(wkb))
	}

	wkbType := binary.LittleEndian.Uint32(wkb[1:5])
	if wkbType != 1001 {
		t.Fatalf("expected WKB type 1001 (PointZ), got %d", wkbType)
	}
}

func TestSdoToWKB_LineString(t *testing.T) {
	geom := &SdoGeometry{
		GType:     2002,
		SRID:      4326,
		ElemInfo:  []int64{1, 2, 1},
		Ordinates: []float64{0, 0, 1, 1, 2, 0},
	}

	wkb, err := SdoToWKB(geom)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Header: 1+4 = 5, numPoints: 4, 3 points * 16 bytes = 48, total = 57
	wkbType := binary.LittleEndian.Uint32(wkb[1:5])
	if wkbType != 2 {
		t.Fatalf("expected WKB type 2 (LineString), got %d", wkbType)
	}

	numPoints := binary.LittleEndian.Uint32(wkb[5:9])
	if numPoints != 3 {
		t.Fatalf("expected 3 points, got %d", numPoints)
	}
}

func TestSdoToWKB_Polygon(t *testing.T) {
	// Simple square polygon
	geom := &SdoGeometry{
		GType:     2003,
		SRID:      4326,
		ElemInfo:  []int64{1, 1003, 1},
		Ordinates: []float64{0, 0, 1, 0, 1, 1, 0, 1, 0, 0},
	}

	wkb, err := SdoToWKB(geom)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wkbType := binary.LittleEndian.Uint32(wkb[1:5])
	if wkbType != 3 {
		t.Fatalf("expected WKB type 3 (Polygon), got %d", wkbType)
	}

	numRings := binary.LittleEndian.Uint32(wkb[5:9])
	if numRings != 1 {
		t.Fatalf("expected 1 ring, got %d", numRings)
	}

	numPoints := binary.LittleEndian.Uint32(wkb[9:13])
	if numPoints != 5 {
		t.Fatalf("expected 5 points, got %d", numPoints)
	}
}

func TestSdoToWKB_Rectangle(t *testing.T) {
	// Optimized rectangle (interp=3)
	geom := &SdoGeometry{
		GType:     2003,
		SRID:      4326,
		ElemInfo:  []int64{1, 1003, 3},
		Ordinates: []float64{0, 0, 10, 20},
	}

	wkb, err := SdoToWKB(geom)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wkbType := binary.LittleEndian.Uint32(wkb[1:5])
	if wkbType != 3 {
		t.Fatalf("expected WKB type 3 (Polygon), got %d", wkbType)
	}

	numRings := binary.LittleEndian.Uint32(wkb[5:9])
	if numRings != 1 {
		t.Fatalf("expected 1 ring, got %d", numRings)
	}

	numPoints := binary.LittleEndian.Uint32(wkb[9:13])
	if numPoints != 5 {
		t.Fatalf("expected 5 points (expanded rectangle), got %d", numPoints)
	}

	// Verify the 5 points form the rectangle
	readPoint := func(offset int) (float64, float64) {
		x := math.Float64frombits(binary.LittleEndian.Uint64(wkb[offset : offset+8]))
		y := math.Float64frombits(binary.LittleEndian.Uint64(wkb[offset+8 : offset+16]))
		return x, y
	}

	x0, y0 := readPoint(13)
	x1, y1 := readPoint(29)
	x2, y2 := readPoint(45)
	x3, y3 := readPoint(61)
	x4, y4 := readPoint(77)

	// (0,0) → (10,0) → (10,20) → (0,20) → (0,0)
	expectPoint(t, "p0", x0, y0, 0, 0)
	expectPoint(t, "p1", x1, y1, 10, 0)
	expectPoint(t, "p2", x2, y2, 10, 20)
	expectPoint(t, "p3", x3, y3, 0, 20)
	expectPoint(t, "p4", x4, y4, 0, 0) // closed ring
}

func TestSdoToWKB_MultiPolygon(t *testing.T) {
	geom := &SdoGeometry{
		GType: 2007,
		SRID:  4326,
		ElemInfo: []int64{
			1, 1003, 1, // first polygon exterior ring
			11, 1003, 1, // second polygon exterior ring
		},
		Ordinates: []float64{
			0, 0, 1, 0, 1, 1, 0, 1, 0, 0, // polygon 1
			5, 5, 6, 5, 6, 6, 5, 6, 5, 5, // polygon 2
		},
	}

	wkb, err := SdoToWKB(geom)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wkbType := binary.LittleEndian.Uint32(wkb[1:5])
	if wkbType != 6 {
		t.Fatalf("expected WKB type 6 (MultiPolygon), got %d", wkbType)
	}

	numGeoms := binary.LittleEndian.Uint32(wkb[5:9])
	if numGeoms != 2 {
		t.Fatalf("expected 2 polygons, got %d", numGeoms)
	}
}

func TestSdoToWKB_Nil(t *testing.T) {
	wkb, err := SdoToWKB(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wkb != nil {
		t.Fatalf("expected nil WKB for nil geometry")
	}
}

func expectPoint(t *testing.T, label string, gotX, gotY, wantX, wantY float64) {
	t.Helper()
	if math.Abs(gotX-wantX) > 1e-10 || math.Abs(gotY-wantY) > 1e-10 {
		t.Errorf("%s: expected (%f,%f), got (%f,%f)", label, wantX, wantY, gotX, gotY)
	}
}
