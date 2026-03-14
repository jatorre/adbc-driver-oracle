package oracle

// SdoPointType represents Oracle's MDSYS.SDO_POINT_TYPE
type SdoPointType struct {
	X float64 `udt:"X"`
	Y float64 `udt:"Y"`
	Z float64 `udt:"Z"`
}

// SdoGeometry represents Oracle's MDSYS.SDO_GEOMETRY
type SdoGeometry struct {
	GType     int64        `udt:"SDO_GTYPE"`
	SRID      int64        `udt:"SDO_SRID"`
	Point     SdoPointType `udt:"SDO_POINT"`
	ElemInfo  []int64      `udt:"SDO_ELEM_INFO"`
	Ordinates []float64    `udt:"SDO_ORDINATES"`
}

// Dimensions returns the number of dimensions (2, 3, or 4) from SDO_GTYPE.
// SDO_GTYPE encoding: DLTT where D=dimensions, L=LRS measure dim, TT=geometry type.
func (g *SdoGeometry) Dimensions() int {
	return int(g.GType / 1000)
}

// GeometryType returns the geometry type code (1-7) from SDO_GTYPE.
//
//	1=Point, 2=Line, 3=Polygon, 4=Collection,
//	5=MultiPoint, 6=MultiLine, 7=MultiPolygon
func (g *SdoGeometry) GeometryType() int {
	return int(g.GType % 100)
}

// LRSDimension returns the LRS measure dimension (0 if none).
func (g *SdoGeometry) LRSDimension() int {
	return int((g.GType / 100) % 10)
}

// IsPointType returns true if this is a simple point using SDO_POINT field
// (SDO_GTYPE=x001 with SDO_POINT populated and empty ElemInfo/Ordinates).
func (g *SdoGeometry) IsPointType() bool {
	return g.GeometryType() == 1 && len(g.ElemInfo) == 0 && len(g.Ordinates) == 0
}
