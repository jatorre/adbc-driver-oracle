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
	"fmt"
	"math"
)

// WKB geometry type constants
const (
	wkbPoint              = 1
	wkbLineString         = 2
	wkbPolygon            = 3
	wkbMultiPoint         = 4
	wkbMultiLineString    = 5
	wkbMultiPolygon       = 6
	wkbGeometryCollection = 7

	wkbPointZ              = 1001
	wkbLineStringZ         = 1002
	wkbPolygonZ            = 1003
	wkbMultiPointZ         = 1004
	wkbMultiLineStringZ    = 1005
	wkbMultiPolygonZ       = 1006
	wkbGeometryCollectionZ = 1007
)

// SDO_ELEM_INFO etype constants
const (
	etypePoint            = 1
	etypeLineString       = 2
	etypePolygonExterior  = 1003
	etypePolygonInterior  = 2003
	etypeCompoundExterior = 1005
	etypeCompoundInterior = 2005
)

// SdoToWKB converts an SdoGeometry struct to Well-Known Binary format.
func SdoToWKB(geom *SdoGeometry) ([]byte, error) {
	if geom == nil {
		return nil, nil
	}

	dims := geom.Dimensions()
	if dims < 2 || dims > 4 {
		return nil, fmt.Errorf("unsupported dimensions: %d", dims)
	}

	gtype := geom.GeometryType()

	switch gtype {
	case 1:
		return buildPointWKB(geom, dims)
	case 2:
		return buildLineStringWKB(geom, dims)
	case 3:
		return buildPolygonWKB(geom, dims)
	case 4:
		return buildCollectionWKB(geom, dims)
	case 5:
		return buildMultiPointWKB(geom, dims)
	case 6:
		return buildMultiLineStringWKB(geom, dims)
	case 7:
		return buildMultiPolygonWKB(geom, dims)
	default:
		return nil, fmt.Errorf("unsupported SDO_GTYPE geometry type: %d", gtype)
	}
}

func wkbTypeCode(baseType int, dims int) uint32 {
	if dims >= 3 {
		return uint32(baseType + 1000)
	}
	return uint32(baseType)
}

func buildPointWKB(geom *SdoGeometry, dims int) ([]byte, error) {
	// Optimized path: use SDO_POINT if available
	if geom.IsPointType() {
		size := 1 + 4 + 8*dims // byte_order + type + coords
		buf := make([]byte, size)
		buf[0] = 1 // little-endian
		binary.LittleEndian.PutUint32(buf[1:5], wkbTypeCode(wkbPoint, dims))
		binary.LittleEndian.PutUint64(buf[5:13], math.Float64bits(geom.Point.X))
		binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(geom.Point.Y))
		if dims >= 3 {
			binary.LittleEndian.PutUint64(buf[21:29], math.Float64bits(geom.Point.Z))
		}
		return buf, nil
	}

	// Point from ordinates
	if len(geom.Ordinates) < dims {
		return nil, fmt.Errorf("not enough ordinates for point: have %d, need %d", len(geom.Ordinates), dims)
	}

	size := 1 + 4 + 8*dims
	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbTypeCode(wkbPoint, dims))
	for d := 0; d < dims; d++ {
		binary.LittleEndian.PutUint64(buf[5+d*8:13+d*8], math.Float64bits(geom.Ordinates[d]))
	}
	return buf, nil
}

func buildLineStringWKB(geom *SdoGeometry, dims int) ([]byte, error) {
	points := len(geom.Ordinates) / dims
	if points*dims != len(geom.Ordinates) {
		return nil, fmt.Errorf("ordinate count %d not divisible by dimensions %d", len(geom.Ordinates), dims)
	}

	// Check elem_info for arc interpretation
	if len(geom.ElemInfo) >= 3 && geom.ElemInfo[2] == 2 {
		// Arc segments — densify
		return buildArcLineStringWKB(geom, dims)
	}

	size := 1 + 4 + 4 + points*8*dims
	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbTypeCode(wkbLineString, dims))
	binary.LittleEndian.PutUint32(buf[5:9], uint32(points))
	writeOrdinates(buf[9:], geom.Ordinates, dims)
	return buf, nil
}

func buildPolygonWKB(geom *SdoGeometry, dims int) ([]byte, error) {
	rings, err := parseRings(geom, dims)
	if err != nil {
		return nil, err
	}

	// Calculate size
	size := 1 + 4 + 4 // byte_order + type + numRings
	for _, ring := range rings {
		points := len(ring) / dims
		size += 4 + points*8*dims // numPoints + coordinates
	}

	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbTypeCode(wkbPolygon, dims))
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(rings)))

	offset := 9
	for _, ring := range rings {
		points := len(ring) / dims
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(points))
		offset += 4
		writeOrdinates(buf[offset:], ring, dims)
		offset += points * 8 * dims
	}

	return buf, nil
}

func buildMultiPointWKB(geom *SdoGeometry, dims int) ([]byte, error) {
	numPoints := len(geom.Ordinates) / dims

	// Header: byte_order + type + numGeometries
	size := 1 + 4 + 4
	pointSize := 1 + 4 + 8*dims
	size += numPoints * pointSize

	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbTypeCode(wkbMultiPoint, dims))
	binary.LittleEndian.PutUint32(buf[5:9], uint32(numPoints))

	offset := 9
	for i := 0; i < numPoints; i++ {
		buf[offset] = 1 // little-endian
		offset++
		binary.LittleEndian.PutUint32(buf[offset:offset+4], wkbTypeCode(wkbPoint, dims))
		offset += 4
		for d := 0; d < dims; d++ {
			binary.LittleEndian.PutUint64(buf[offset:offset+8], math.Float64bits(geom.Ordinates[i*dims+d]))
			offset += 8
		}
	}

	return buf, nil
}

func buildMultiLineStringWKB(geom *SdoGeometry, dims int) ([]byte, error) {
	lines, err := parseElements(geom, dims, etypeLineString)
	if err != nil {
		return nil, err
	}

	// Calculate total size
	size := 1 + 4 + 4 // header
	for _, line := range lines {
		points := len(line) / dims
		size += 1 + 4 + 4 + points*8*dims // sub-geometry header + points
	}

	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbTypeCode(wkbMultiLineString, dims))
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(lines)))

	offset := 9
	for _, line := range lines {
		points := len(line) / dims
		buf[offset] = 1
		offset++
		binary.LittleEndian.PutUint32(buf[offset:offset+4], wkbTypeCode(wkbLineString, dims))
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(points))
		offset += 4
		writeOrdinates(buf[offset:], line, dims)
		offset += points * 8 * dims
	}

	return buf, nil
}

func buildMultiPolygonWKB(geom *SdoGeometry, dims int) ([]byte, error) {
	polygons, err := parsePolygons(geom, dims)
	if err != nil {
		return nil, err
	}

	// Calculate total size
	size := 1 + 4 + 4 // header
	for _, polygon := range polygons {
		polySize := 1 + 4 + 4 // sub-geometry header + numRings
		for _, ring := range polygon {
			points := len(ring) / dims
			polySize += 4 + points*8*dims
		}
		size += polySize
	}

	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbTypeCode(wkbMultiPolygon, dims))
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(polygons)))

	offset := 9
	for _, polygon := range polygons {
		buf[offset] = 1
		offset++
		binary.LittleEndian.PutUint32(buf[offset:offset+4], wkbTypeCode(wkbPolygon, dims))
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(polygon)))
		offset += 4
		for _, ring := range polygon {
			points := len(ring) / dims
			binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(points))
			offset += 4
			writeOrdinates(buf[offset:], ring, dims)
			offset += points * 8 * dims
		}
	}

	return buf, nil
}

func buildCollectionWKB(geom *SdoGeometry, dims int) ([]byte, error) {
	// For geometry collections, we need to parse each sub-geometry from ElemInfo.
	// This is the most complex case. For now, return an error for unsupported collections.
	return nil, fmt.Errorf("GeometryCollection (SDO_GTYPE=x004) not yet supported")
}

// parseRings extracts polygon rings from SDO_ELEM_INFO and SDO_ORDINATES.
func parseRings(geom *SdoGeometry, dims int) ([][]float64, error) {
	if len(geom.ElemInfo) < 3 {
		return nil, fmt.Errorf("invalid elem_info for polygon: length %d", len(geom.ElemInfo))
	}

	var rings [][]float64
	numTriplets := len(geom.ElemInfo) / 3

	for i := 0; i < numTriplets; i++ {
		startOffset := int(geom.ElemInfo[i*3]) - 1 // 1-based to 0-based
		etype := int(geom.ElemInfo[i*3+1])
		interp := int(geom.ElemInfo[i*3+2])

		if etype != etypePolygonExterior && etype != etypePolygonInterior {
			continue
		}

		// Determine end of this element's ordinates
		var endOffset int
		if i+1 < numTriplets {
			endOffset = int(geom.ElemInfo[(i+1)*3]) - 1
		} else {
			endOffset = len(geom.Ordinates)
		}

		ordinates := geom.Ordinates[startOffset:endOffset]

		switch interp {
		case 1:
			// Straight-line segments
			rings = append(rings, ordinates)
		case 3:
			// Optimized rectangle: 2 corner points → 5-point ring
			ring, err := expandRectangle(ordinates, dims)
			if err != nil {
				return nil, err
			}
			rings = append(rings, ring)
		case 2:
			// Arc segments — densify to straight segments
			ring := densifyArcRing(ordinates, dims)
			rings = append(rings, ring)
		default:
			return nil, fmt.Errorf("unsupported polygon interpretation: %d", interp)
		}
	}

	if len(rings) == 0 {
		return nil, fmt.Errorf("no polygon rings found in elem_info")
	}

	return rings, nil
}

// expandRectangle converts an optimized rectangle (2 points) to a 5-point ring.
func expandRectangle(ordinates []float64, dims int) ([]float64, error) {
	if len(ordinates) != 2*dims {
		return nil, fmt.Errorf("rectangle expects %d ordinates, got %d", 2*dims, len(ordinates))
	}

	x1, y1 := ordinates[0], ordinates[1]
	x2, y2 := ordinates[dims], ordinates[dims+1]

	// Build 5-point ring: (x1,y1) → (x2,y1) → (x2,y2) → (x1,y2) → (x1,y1)
	ring := make([]float64, 5*dims)
	setPoint := func(idx int, x, y float64) {
		ring[idx*dims] = x
		ring[idx*dims+1] = y
		// Copy Z and higher dimensions from first point
		for d := 2; d < dims; d++ {
			ring[idx*dims+d] = ordinates[d]
		}
	}

	setPoint(0, x1, y1)
	setPoint(1, x2, y1)
	setPoint(2, x2, y2)
	setPoint(3, x1, y2)
	setPoint(4, x1, y1)

	return ring, nil
}

// parseElements extracts line string elements from SDO_ELEM_INFO.
func parseElements(geom *SdoGeometry, dims int, targetEtype int) ([][]float64, error) {
	if len(geom.ElemInfo) < 3 {
		return nil, fmt.Errorf("invalid elem_info: length %d", len(geom.ElemInfo))
	}

	var elements [][]float64
	numTriplets := len(geom.ElemInfo) / 3

	for i := 0; i < numTriplets; i++ {
		startOffset := int(geom.ElemInfo[i*3]) - 1
		etype := int(geom.ElemInfo[i*3+1])

		if etype != targetEtype {
			continue
		}

		var endOffset int
		if i+1 < numTriplets {
			endOffset = int(geom.ElemInfo[(i+1)*3]) - 1
		} else {
			endOffset = len(geom.Ordinates)
		}

		elements = append(elements, geom.Ordinates[startOffset:endOffset])
	}

	return elements, nil
}

// parsePolygons groups rings into polygons (exterior + interior rings).
func parsePolygons(geom *SdoGeometry, dims int) ([][][]float64, error) {
	if len(geom.ElemInfo) < 3 {
		return nil, fmt.Errorf("invalid elem_info for multi-polygon: length %d", len(geom.ElemInfo))
	}

	var polygons [][][]float64
	var currentPolygon [][]float64
	numTriplets := len(geom.ElemInfo) / 3

	for i := 0; i < numTriplets; i++ {
		startOffset := int(geom.ElemInfo[i*3]) - 1
		etype := int(geom.ElemInfo[i*3+1])
		interp := int(geom.ElemInfo[i*3+2])

		var endOffset int
		if i+1 < numTriplets {
			endOffset = int(geom.ElemInfo[(i+1)*3]) - 1
		} else {
			endOffset = len(geom.Ordinates)
		}

		ordinates := geom.Ordinates[startOffset:endOffset]

		if etype == etypePolygonExterior {
			// Start new polygon
			if currentPolygon != nil {
				polygons = append(polygons, currentPolygon)
			}
			currentPolygon = nil

			var ring []float64
			var err error
			switch interp {
			case 1:
				ring = ordinates
			case 3:
				ring, err = expandRectangle(ordinates, dims)
				if err != nil {
					return nil, err
				}
			case 2:
				ring = densifyArcRing(ordinates, dims)
			default:
				return nil, fmt.Errorf("unsupported exterior ring interpretation: %d", interp)
			}
			currentPolygon = append(currentPolygon, ring)

		} else if etype == etypePolygonInterior {
			var ring []float64
			var err error
			switch interp {
			case 1:
				ring = ordinates
			case 3:
				ring, err = expandRectangle(ordinates, dims)
				if err != nil {
					return nil, err
				}
			case 2:
				ring = densifyArcRing(ordinates, dims)
			default:
				return nil, fmt.Errorf("unsupported interior ring interpretation: %d", interp)
			}
			currentPolygon = append(currentPolygon, ring)
		}
	}

	if currentPolygon != nil {
		polygons = append(polygons, currentPolygon)
	}

	return polygons, nil
}

// writeOrdinates writes ordinate values as little-endian float64s into buf.
func writeOrdinates(buf []byte, ordinates []float64, dims int) {
	for i, v := range ordinates {
		binary.LittleEndian.PutUint64(buf[i*8:(i+1)*8], math.Float64bits(v))
	}
}

// buildArcLineStringWKB densifies arc segments and returns a WKB line string.
func buildArcLineStringWKB(geom *SdoGeometry, dims int) ([]byte, error) {
	densified := densifyArcRing(geom.Ordinates, dims)
	points := len(densified) / dims

	size := 1 + 4 + 4 + points*8*dims
	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], wkbTypeCode(wkbLineString, dims))
	binary.LittleEndian.PutUint32(buf[5:9], uint32(points))
	writeOrdinates(buf[9:], densified, dims)
	return buf, nil
}

// densifyArcRing converts arc-based ordinates (3-point arcs) to straight segments.
// Oracle uses 3-point arcs: (start, mid, end) for each arc.
// We interpolate each arc with a configurable number of segments.
const arcSegments = 32

func densifyArcRing(ordinates []float64, dims int) []float64 {
	numPoints := len(ordinates) / dims
	if numPoints < 3 {
		return ordinates
	}

	var result []float64

	// Process arcs: each arc is 3 consecutive points (start, mid, end).
	// Arcs share endpoints, so we step by 2 points per arc.
	for i := 0; i+2*dims+1 < len(ordinates); i += 2 * dims {
		x1, y1 := ordinates[i], ordinates[i+1]
		xm, ym := ordinates[i+dims], ordinates[i+dims+1]
		x2, y2 := ordinates[i+2*dims], ordinates[i+2*dims+1]

		arcPoints := interpolateArc(x1, y1, xm, ym, x2, y2, arcSegments)

		// Skip first point if not the first arc (to avoid duplicates)
		startIdx := 0
		if i > 0 {
			startIdx = 1
		}

		for j := startIdx; j < len(arcPoints); j++ {
			result = append(result, arcPoints[j][0], arcPoints[j][1])
			// Add Z and higher dimensions from the start point
			for d := 2; d < dims; d++ {
				result = append(result, ordinates[i+d])
			}
		}
	}

	return result
}

// interpolateArc generates points along a circular arc defined by 3 points.
func interpolateArc(x1, y1, xm, ym, x2, y2 float64, segments int) [][2]float64 {
	// Find the circumscribed circle center
	ax, ay := x1, y1
	bx, by := xm, ym
	cx, cy := x2, y2

	D := 2 * (ax*(by-cy) + bx*(cy-ay) + cx*(ay-by))

	if math.Abs(D) < 1e-10 {
		// Points are collinear, return straight line
		return [][2]float64{{x1, y1}, {xm, ym}, {x2, y2}}
	}

	ux := ((ax*ax+ay*ay)*(by-cy) + (bx*bx+by*by)*(cy-ay) + (cx*cx+cy*cy)*(ay-by)) / D
	uy := ((ax*ax+ay*ay)*(cx-bx) + (bx*bx+by*by)*(ax-cx) + (cx*cx+cy*cy)*(bx-ax)) / D

	radius := math.Sqrt((ax-ux)*(ax-ux) + (ay-uy)*(ay-uy))

	// Calculate angles
	a1 := math.Atan2(ay-uy, ax-ux)
	am := math.Atan2(by-uy, bx-ux)
	a2 := math.Atan2(cy-uy, cx-ux)

	// Determine arc direction (CW or CCW)
	// The midpoint angle must be between start and end
	sweep := normalizeAngle(a2 - a1)
	midSweep := normalizeAngle(am - a1)

	// If midpoint is not in the sweep direction, reverse
	if sweep > 0 && midSweep < 0 {
		sweep -= 2 * math.Pi
	} else if sweep < 0 && midSweep > 0 {
		sweep += 2 * math.Pi
	}

	points := make([][2]float64, segments+1)
	for i := 0; i <= segments; i++ {
		t := float64(i) / float64(segments)
		angle := a1 + t*sweep
		points[i] = [2]float64{
			ux + radius*math.Cos(angle),
			uy + radius*math.Sin(angle),
		}
	}

	return points
}

func normalizeAngle(a float64) float64 {
	for a > math.Pi {
		a -= 2 * math.Pi
	}
	for a < -math.Pi {
		a += 2 * math.Pi
	}
	return a
}
