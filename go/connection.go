package oracle

import (
	"context"
	"database/sql"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase
	db *sql.DB
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	stBase := driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper)
	st := &statementImpl{
		StatementImplBase: stBase,
		cnxn:              c,
		alloc:             c.Alloc,
	}
	return driverbase.NewStatement(st), nil
}

// --- Metadata: DbObjectsEnumerator ---

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	// Oracle doesn't have catalogs in the same way. Return the current database name.
	var name string
	err := c.db.QueryRowContext(ctx, "SELECT ORA_DATABASE_NAME FROM DUAL").Scan(&name)
	if err != nil {
		return nil, err
	}
	if catalogFilter != nil && !matchLike(name, *catalogFilter) {
		return nil, nil
	}
	return []string{name}, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	query := "SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME"
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if schemaFilter != nil && !matchLike(name, *schemaFilter) {
			continue
		}
		schemas = append(schemas, name)
	}
	return schemas, rows.Err()
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	query := `SELECT TABLE_NAME, TABLE_TYPE FROM (
		SELECT TABLE_NAME, 'TABLE' AS TABLE_TYPE FROM ALL_TABLES WHERE OWNER = :1
		UNION ALL
		SELECT VIEW_NAME, 'VIEW' AS TABLE_TYPE FROM ALL_VIEWS WHERE OWNER = :1
	) ORDER BY TABLE_NAME`

	rows, err := c.db.QueryContext(ctx, query, strings.ToUpper(schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []driverbase.TableInfo
	for rows.Next() {
		var name, tableType string
		if err := rows.Scan(&name, &tableType); err != nil {
			return nil, err
		}
		if tableFilter != nil && !matchLike(name, *tableFilter) {
			continue
		}

		info := driverbase.TableInfo{
			TableName: name,
			TableType: tableType,
		}

		if includeColumns {
			cols, err := c.getColumnsForTable(ctx, schema, name, columnFilter)
			if err != nil {
				return nil, err
			}
			info.TableColumns = cols
		}

		tables = append(tables, info)
	}
	return tables, rows.Err()
}

func (c *connectionImpl) getColumnsForTable(ctx context.Context, schema, table string, columnFilter *string) ([]driverbase.ColumnInfo, error) {
	query := `SELECT COLUMN_NAME, COLUMN_ID, DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE
		FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 ORDER BY COLUMN_ID`
	rows, err := c.db.QueryContext(ctx, query, strings.ToUpper(schema), strings.ToUpper(table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []driverbase.ColumnInfo
	for rows.Next() {
		var (
			name      string
			ordinal   sql.NullInt32
			dataType  sql.NullString
			nullable  sql.NullString
			colDef    sql.NullString
			precision sql.NullInt16
			scale     sql.NullInt16
		)
		if err := rows.Scan(&name, &ordinal, &dataType, &nullable, &colDef, &precision, &scale); err != nil {
			return nil, err
		}
		if columnFilter != nil && !matchLike(name, *columnFilter) {
			continue
		}

		xdbcNullable := driverbase.XdbcColumnNullableUnknown
		if nullable.Valid && nullable.String == "Y" {
			xdbcNullable = driverbase.XdbcColumnNullable
		} else if nullable.Valid && nullable.String == "N" {
			xdbcNullable = driverbase.XdbcColumnNoNulls
		}

		col := driverbase.ColumnInfo{
			ColumnName:      name,
			OrdinalPosition: driverbase.NullInt32ToPtr(ordinal),
			XdbcTypeName:    driverbase.NullStringToPtr(dataType),
			XdbcNullable:    &xdbcNullable,
			XdbcColumnDef:   driverbase.NullStringToPtr(colDef),
		}
		if precision.Valid {
			col.XdbcColumnSize = driverbase.ToPtr[int32](int32(precision.Int16))
		}
		if scale.Valid {
			col.XdbcDecimalDigits = driverbase.NullInt16ToPtr(scale)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// --- Metadata: TableTypeLister ---

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM"}, nil
}

// --- Metadata: GetTableSchema ---

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	schema := "USER"
	if dbSchema != nil {
		schema = strings.ToUpper(*dbSchema)
	}

	query := `SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_PRECISION, DATA_SCALE
		FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 ORDER BY COLUMN_ID`
	rows, err := c.db.QueryContext(ctx, query, schema, strings.ToUpper(tableName))
	if err != nil {
		return nil, c.ErrorHelper.Errorf(adbc.StatusIO, "GetTableSchema: %s", err)
	}
	defer rows.Close()

	var fields []arrow.Field
	for rows.Next() {
		var (
			name     string
			dataType string
			nullable string
			prec     sql.NullInt64
			scale    sql.NullInt64
		)
		if err := rows.Scan(&name, &dataType, &nullable, &prec, &scale); err != nil {
			return nil, err
		}

		arrowType := oracleDataTypeToArrow(dataType, prec, scale)
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     arrowType,
			Nullable: nullable == "Y",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, c.ErrorHelper.Errorf(adbc.StatusNotFound, "table %s.%s not found", schema, tableName)
	}

	return arrow.NewSchema(fields, nil), nil
}

// --- Namespace: CurrentNamespacer ---

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	var name string
	err := c.db.QueryRow("SELECT ORA_DATABASE_NAME FROM DUAL").Scan(&name)
	return name, err
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	var name string
	err := c.db.QueryRow("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL").Scan(&name)
	return name, err
}

func (c *connectionImpl) SetCurrentCatalog(catalog string) error {
	// Oracle doesn't support switching catalogs
	return c.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Oracle does not support switching catalogs")
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	_, err := c.db.Exec("ALTER SESSION SET CURRENT_SCHEMA = " + strings.ToUpper(schema))
	if err != nil {
		return c.ErrorHelper.Errorf(adbc.StatusIO, "failed to set schema: %s", err)
	}
	return nil
}

// --- Transactions ---

func (c *connectionImpl) Commit(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, "COMMIT")
	if err != nil {
		return c.ErrorHelper.Errorf(adbc.StatusIO, "commit failed: %s", err)
	}
	return nil
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, "ROLLBACK")
	if err != nil {
		return c.ErrorHelper.Errorf(adbc.StatusIO, "rollback failed: %s", err)
	}
	return nil
}

func (c *connectionImpl) Close() error {
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

// --- Helpers ---

// oracleDataTypeToArrow converts an Oracle data type name to an Arrow type.
func oracleDataTypeToArrow(dataType string, precision, scale sql.NullInt64) arrow.DataType {
	dt := strings.ToUpper(dataType)
	switch {
	case dt == "NUMBER" || dt == "FLOAT" || dt == "BINARY_FLOAT" || dt == "BINARY_DOUBLE":
		if scale.Valid && scale.Int64 == 0 && precision.Valid && precision.Int64 > 0 && precision.Int64 <= 18 {
			return arrow.PrimitiveTypes.Int64
		}
		return arrow.PrimitiveTypes.Float64
	case dt == "VARCHAR2" || dt == "VARCHAR" || dt == "NVARCHAR2" ||
		dt == "CHAR" || dt == "NCHAR" || dt == "CLOB" || dt == "NCLOB" ||
		dt == "LONG" || dt == "ROWID":
		return arrow.BinaryTypes.String
	case dt == "DATE" || dt == "TIMESTAMP" || strings.HasPrefix(dt, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us
	case dt == "RAW" || dt == "LONG RAW" || dt == "BLOB":
		return arrow.BinaryTypes.Binary
	case dt == "SDO_GEOMETRY":
		return arrow.BinaryTypes.Binary
	default:
		return arrow.BinaryTypes.String
	}
}

// matchLike performs SQL LIKE-style matching (% wildcard only).
func matchLike(value, pattern string) bool {
	v := strings.ToUpper(value)
	p := strings.ToUpper(pattern)

	if p == "%" {
		return true
	}
	if !strings.Contains(p, "%") {
		return v == p
	}

	parts := strings.Split(p, "%")
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(v[pos:], part)
		if idx < 0 {
			return false
		}
		if i == 0 && idx != 0 {
			return false // must match start if pattern doesn't start with %
		}
		pos += idx + len(part)
	}
	// If pattern doesn't end with %, must match end
	if !strings.HasSuffix(p, "%") && pos != len(v) {
		return false
	}
	return true
}
