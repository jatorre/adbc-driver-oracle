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
