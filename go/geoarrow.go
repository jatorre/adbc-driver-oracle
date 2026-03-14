package oracle

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

const (
	GeoArrowExtensionName = "geoarrow.wkb"
)

// GeoArrowWKBField creates an Arrow field with geoarrow.wkb extension metadata.
func GeoArrowWKBField(name string, srid int64, nullable bool) arrow.Field {
	md := arrow.MetadataFrom(map[string]string{
		"ARROW:extension:name":     GeoArrowExtensionName,
		"ARROW:extension:metadata": buildGeoArrowMetadata(srid),
	})

	return arrow.Field{
		Name:     name,
		Type:     arrow.BinaryTypes.Binary,
		Nullable: nullable,
		Metadata: md,
	}
}

// buildGeoArrowMetadata builds the PROJJSON CRS metadata for a given SRID.
func buildGeoArrowMetadata(srid int64) string {
	if srid == 0 {
		return "{}"
	}

	// Minimal PROJJSON with EPSG authority
	return fmt.Sprintf(
		`{"columns":{"geometry":{"encoding":"wkb","crs":{"id":{"authority":"EPSG","code":%d}}}}}`,
		srid,
	)
}

// UpdateSchemaWithGeoArrow replaces a Binary field with a geoarrow.wkb-annotated field.
func UpdateSchemaWithGeoArrow(schema *arrow.Schema, fieldIdx int, srid int64) *arrow.Schema {
	fields := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		if i == fieldIdx {
			fields[i] = GeoArrowWKBField(schema.Field(i).Name, srid, schema.Field(i).Nullable)
		} else {
			fields[i] = schema.Field(i)
		}
	}
	md := schema.Metadata()
	return arrow.NewSchema(fields, &md)
}
