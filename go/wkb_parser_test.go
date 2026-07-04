// Copyright 2025 CARTO
package oracle

import (
	"testing"
)

func TestWKBRoundTrip_Point(t *testing.T) {
	orig := &SdoGeometry{GType: 2001, SRID: 4326, Point: SdoPointType{X: -73.935242, Y: 40.730610}}
	wkb, err := SdoToWKB(orig)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := WKBToSdo(wkb, 4326)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.GType != 2001 {
		t.Errorf("GType: want 2001, got %d", parsed.GType)
	}
	if parsed.Point.X != orig.Point.X || parsed.Point.Y != orig.Point.Y {
		t.Errorf("Point: want (%f,%f), got (%f,%f)", orig.Point.X, orig.Point.Y, parsed.Point.X, parsed.Point.Y)
	}
}

func TestWKBRoundTrip_Polygon(t *testing.T) {
	orig := &SdoGeometry{
		GType: 2003, SRID: 4326,
		ElemInfo:  []int64{1, 1003, 1},
		Ordinates: []float64{0, 0, 1, 0, 1, 1, 0, 1, 0, 0},
	}
	wkb, err := SdoToWKB(orig)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := WKBToSdo(wkb, 4326)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.GType != 2003 {
		t.Errorf("GType: want 2003, got %d", parsed.GType)
	}
	if len(parsed.Ordinates) != 10 {
		t.Errorf("Ordinates: want 10, got %d", len(parsed.Ordinates))
	}
	if len(parsed.ElemInfo) != 3 {
		t.Errorf("ElemInfo: want 3, got %d", len(parsed.ElemInfo))
	}
}

func TestWKBRoundTrip_MultiPolygon(t *testing.T) {
	orig := &SdoGeometry{
		GType: 2007, SRID: 4326,
		ElemInfo: []int64{1, 1003, 1, 11, 1003, 1},
		Ordinates: []float64{
			0, 0, 1, 0, 1, 1, 0, 1, 0, 0,
			5, 5, 6, 5, 6, 6, 5, 6, 5, 5,
		},
	}
	wkb, err := SdoToWKB(orig)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := WKBToSdo(wkb, 4326)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.GType != 2007 {
		t.Errorf("GType: want 2007, got %d", parsed.GType)
	}
	if len(parsed.Ordinates) != 20 {
		t.Errorf("Ordinates: want 20, got %d", len(parsed.Ordinates))
	}
}

// Corrupt WKB with a wire-controlled point count far beyond the actual data
// must return an error, not panic or allocate gigabytes (the conversion runs
// in the ingest pipeline goroutine, where a panic kills the whole process).
func TestWKBToSdo_TruncatedData(t *testing.T) {
	cases := map[string][]byte{
		"linestring claims 1M points": {
			1, 2, 0, 0, 0, // little-endian, LineString
			0, 0, 16, 0, // numPoints = 1048576
			0, 0, 0, 0, 0, 0, 0, 0, // just one ordinate
		},
		"polygon ring claims huge count": {
			1, 3, 0, 0, 0, // Polygon
			1, 0, 0, 0, // one ring
			255, 255, 255, 255, // numPoints = 4294967295
		},
		"multilinestring truncated": {
			1, 5, 0, 0, 0, // MultiLineString
			1, 0, 0, 0, // one sub-geometry
			1, 2, 0, 0, 0, // sub: LineString
			100, 0, 0, 0, // 100 points, no data
		},
	}
	for name, wkb := range cases {
		if _, err := WKBToSdo(wkb, 4326); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}
