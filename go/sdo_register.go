package oracle

import (
	"database/sql"
	"fmt"

	go_ora "github.com/sijms/go-ora/v2"
)

// RegisterSDOTypes registers all MDSYS.SDO_GEOMETRY related types with go-ora
// so that geometry columns can be scanned directly into Go structs.
//
// Registration order matters — inner types must be registered before outer types:
//  1. SDO_ELEM_INFO_ARRAY (VARRAY OF NUMBER)
//  2. SDO_ORDINATE_ARRAY (VARRAY OF NUMBER)
//  3. SDO_POINT_TYPE (struct: X, Y, Z)
//  4. SDO_GEOMETRY (struct containing the above)
func RegisterSDOTypes(db *sql.DB) error {
	// Register VARRAY types as arrays of NUMBER
	err := go_ora.RegisterTypeWithOwner(db, "MDSYS", "NUMBER", "SDO_ELEM_INFO_ARRAY", nil)
	if err != nil {
		return fmt.Errorf("register SDO_ELEM_INFO_ARRAY: %w", err)
	}

	err = go_ora.RegisterTypeWithOwner(db, "MDSYS", "NUMBER", "SDO_ORDINATE_ARRAY", nil)
	if err != nil {
		return fmt.Errorf("register SDO_ORDINATE_ARRAY: %w", err)
	}

	// Register SDO_POINT_TYPE struct
	err = go_ora.RegisterTypeWithOwner(db, "MDSYS", "SDO_POINT_TYPE", "", SdoPointType{})
	if err != nil {
		return fmt.Errorf("register SDO_POINT_TYPE: %w", err)
	}

	// Register SDO_GEOMETRY struct
	err = go_ora.RegisterTypeWithOwner(db, "MDSYS", "SDO_GEOMETRY", "", SdoGeometry{})
	if err != nil {
		return fmt.Errorf("register SDO_GEOMETRY: %w", err)
	}

	return nil
}
