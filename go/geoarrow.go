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

// buildGeoArrowMetadata builds the GeoArrow column-level CRS metadata for a
// given SRID. The compact `{"crs":"EPSG:N"}` form is what DuckDB produces and
// what the GeoArrow spec accepts as the column-level extension metadata.
// Returns an empty JSON object when SRID is unknown so the column is still
// tagged as geoarrow.wkb but without a CRS claim.
func buildGeoArrowMetadata(srid int64) string {
	if srid == 0 {
		return "{}"
	}
	return fmt.Sprintf(`{"crs":"EPSG:%d"}`, srid)
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
