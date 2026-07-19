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

// WKBToSdo converts Well-Known Binary bytes to an SdoGeometry struct.
// This is the reverse of SdoToWKB.
func WKBToSdo(wkb []byte, srid int64) (*SdoGeometry, error) {
	if len(wkb) < 5 {
		return nil, fmt.Errorf("WKB too short: %d bytes", len(wkb))
	}

	var bo binary.ByteOrder
	switch wkb[0] {
	case 0:
		bo = binary.BigEndian
	case 1:
		bo = binary.LittleEndian
	default:
		return nil, fmt.Errorf("invalid WKB byte order: %d", wkb[0])
	}

	wkbType := bo.Uint32(wkb[1:5])
	hasZ := wkbType >= 1000 && wkbType < 2000
	baseType := wkbType
	if hasZ {
		baseType -= 1000
	}

	dims := 2
	if hasZ {
		dims = 3
	}

	geom := &SdoGeometry{SRID: srid}

	switch baseType {
	case 1: // Point
		return parseWKBPoint(wkb[5:], bo, dims, srid)
	case 2: // LineString
		return parseWKBLineString(wkb[5:], bo, dims, srid)
	case 3: // Polygon
		return parseWKBPolygon(wkb[5:], bo, dims, srid)
	case 4: // MultiPoint
		return parseWKBMulti(wkb[5:], bo, dims, srid, 5)
	case 5: // MultiLineString
		return parseWKBMulti(wkb[5:], bo, dims, srid, 6)
	case 6: // MultiPolygon
		return parseWKBMulti(wkb[5:], bo, dims, srid, 7)
	default:
		_ = geom
		return nil, fmt.Errorf("unsupported WKB type: %d", wkbType)
	}
}

func readFloat64(data []byte, offset int, bo binary.ByteOrder) float64 {
	return math.Float64frombits(bo.Uint64(data[offset : offset+8]))
}

func parseWKBPoint(data []byte, bo binary.ByteOrder, dims int, srid int64) (*SdoGeometry, error) {
	if len(data) < dims*8 {
		return nil, fmt.Errorf("WKB point data too short")
	}
	geom := &SdoGeometry{
		GType: int64(dims)*1000 + 1,
		SRID:  srid,
		Point: SdoPointType{
			X: readFloat64(data, 0, bo),
			Y: readFloat64(data, 8, bo),
		},
	}
	if dims >= 3 {
		geom.Point.Z = readFloat64(data, 16, bo)
	}
	return geom, nil
}

func parseWKBLineString(data []byte, bo binary.ByteOrder, dims int, srid int64) (*SdoGeometry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("WKB linestring data too short")
	}
	numPoints := int(bo.Uint32(data[0:4]))
	ords, err := readOrdinates(data[4:], bo, numPoints, dims)
	if err != nil {
		return nil, err
	}

	return &SdoGeometry{
		GType:     int64(dims)*1000 + 2,
		SRID:      srid,
		ElemInfo:  []int64{1, 2, 1},
		Ordinates: ords,
	}, nil
}

func parseWKBPolygon(data []byte, bo binary.ByteOrder, dims int, srid int64) (*SdoGeometry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("WKB polygon data too short")
	}
	numRings := int(bo.Uint32(data[0:4]))
	offset := 4

	var elemInfo []int64
	var ordinates []float64

	for ring := 0; ring < numRings; ring++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("WKB polygon ring header truncated")
		}
		numPoints := int(bo.Uint32(data[offset : offset+4]))
		offset += 4

		etype := int64(1003) // exterior
		if ring > 0 {
			etype = 2003 // interior (hole)
		}
		elemInfo = append(elemInfo, int64(len(ordinates)+1), etype, 1)

		ords, err := readOrdinates(data[offset:], bo, numPoints, dims)
		if err != nil {
			return nil, err
		}
		ordinates = append(ordinates, orientRing(ords, dims, ring == 0)...)
		offset += numPoints * dims * 8
	}

	return &SdoGeometry{
		GType:     int64(dims)*1000 + 3,
		SRID:      srid,
		ElemInfo:  elemInfo,
		Ordinates: ordinates,
	}, nil
}

func parseWKBMulti(data []byte, bo binary.ByteOrder, dims int, srid int64, sdoGType int) (*SdoGeometry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("WKB multi data too short")
	}
	numGeoms := int(bo.Uint32(data[0:4]))
	offset := 4

	var elemInfo []int64
	var ordinates []float64

	for i := 0; i < numGeoms; i++ {
		if offset+5 > len(data) {
			return nil, fmt.Errorf("WKB sub-geometry header truncated")
		}
		// Each sub-geometry has its own byte order + type header
		var subBO binary.ByteOrder
		if data[offset] == 0 {
			subBO = binary.BigEndian
		} else {
			subBO = binary.LittleEndian
		}
		subType := subBO.Uint32(data[offset+1 : offset+5])
		offset += 5

		// Strip Z flag from subType for comparison
		baseSubType := subType
		if baseSubType >= 1000 {
			baseSubType -= 1000
		}

		switch baseSubType {
		case 1: // Point in MultiPoint
			if offset+dims*8 > len(data) {
				return nil, fmt.Errorf("WKB multi point data truncated")
			}
			elemInfo = append(elemInfo, int64(len(ordinates)+1), 1, 1)
			ords, err := readOrdinates(data[offset:], subBO, 1, dims)
			if err != nil {
				return nil, err
			}
			ordinates = append(ordinates, ords...)
			offset += dims * 8

		case 2: // LineString in MultiLineString
			if offset+4 > len(data) {
				return nil, fmt.Errorf("WKB multi linestring data truncated")
			}
			numPoints := int(subBO.Uint32(data[offset : offset+4]))
			offset += 4
			elemInfo = append(elemInfo, int64(len(ordinates)+1), 2, 1)
			ords, err := readOrdinates(data[offset:], subBO, numPoints, dims)
			if err != nil {
				return nil, err
			}
			ordinates = append(ordinates, ords...)
			offset += numPoints * dims * 8

		case 3: // Polygon in MultiPolygon
			if offset+4 > len(data) {
				return nil, fmt.Errorf("WKB multi polygon data truncated")
			}
			numRings := int(subBO.Uint32(data[offset : offset+4]))
			offset += 4
			for ring := 0; ring < numRings; ring++ {
				if offset+4 > len(data) {
					return nil, fmt.Errorf("WKB multi polygon ring truncated")
				}
				numPoints := int(subBO.Uint32(data[offset : offset+4]))
				offset += 4

				etype := int64(1003)
				if ring > 0 {
					etype = 2003
				}
				elemInfo = append(elemInfo, int64(len(ordinates)+1), etype, 1)
				ords, err := readOrdinates(data[offset:], subBO, numPoints, dims)
				if err != nil {
					return nil, err
				}
				ordinates = append(ordinates, orientRing(ords, dims, ring == 0)...)
				offset += numPoints * dims * 8
			}

		default:
			return nil, fmt.Errorf("unsupported sub-geometry type in multi: %d", subType)
		}
	}

	return &SdoGeometry{
		GType:     int64(dims)*1000 + int64(sdoGType),
		SRID:      srid,
		ElemInfo:  elemInfo,
		Ordinates: ordinates,
	}, nil
}

// orientRing enforces Oracle's SDO ring convention — exterior rings
// counter-clockwise, interior rings (holes) clockwise — reversing the point
// order in place when the winding disagrees (ORA-13367 otherwise). WKB
// carries no winding guarantee, so this must be normalized on conversion.
func orientRing(ords []float64, dims int, exterior bool) []float64 {
	n := len(ords) / dims
	if n < 3 {
		return ords
	}
	// Shoelace signed area over (x, y); positive = counter-clockwise.
	var area float64
	for i := 0; i < n-1; i++ {
		x1, y1 := ords[i*dims], ords[i*dims+1]
		x2, y2 := ords[(i+1)*dims], ords[(i+1)*dims+1]
		area += x1*y2 - x2*y1
	}
	if (area > 0) == exterior || area == 0 {
		return ords
	}
	for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
		for d := 0; d < dims; d++ {
			ords[i*dims+d], ords[j*dims+d] = ords[j*dims+d], ords[i*dims+d]
		}
	}
	return ords
}

func readOrdinates(data []byte, bo binary.ByteOrder, numPoints, dims int) ([]float64, error) {
	// numPoints comes off the wire; validate against the remaining bytes
	// before allocating or indexing (a corrupt count would otherwise panic
	// or allocate gigabytes).
	if numPoints < 0 || dims <= 0 || numPoints > len(data)/(dims*8) {
		return nil, fmt.Errorf("WKB ordinate data truncated: %d points × %d dims, %d bytes available", numPoints, dims, len(data))
	}
	ords := make([]float64, numPoints*dims)
	for i := range ords {
		ords[i] = readFloat64(data, i*8, bo)
	}
	return ords, nil
}
