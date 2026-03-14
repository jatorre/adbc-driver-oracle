package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// geomColumnInfo describes an SDO_GEOMETRY column detected in a query.
type geomColumnInfo struct {
	OriginalName string // original column name in the result set
	TableAlias   string // table alias used in rewritten query
	FieldIndex   int    // index of the gtype field in the rewritten result set
}

const geomFieldsPerColumn = 7

// queryRewriteResult holds the rewritten query and geometry column metadata.
type queryRewriteResult struct {
	RewrittenQuery string
	GeomColumns    []geomColumnInfo
	OriginalSchema []string // original column names in order
}

type columnMeta struct {
	Name     string
	DataType string
}

// detectAndRewriteQuery checks if a query returns SDO_GEOMETRY columns and
// rewrites it to decompose them into scalar fields for client-side WKB construction.
//
// Detection uses Oracle's data dictionary (ALL_TAB_COLUMNS) to find geometry columns.
// go-ora panics on SDO_GEOMETRY types, so we cannot probe column types via a query.
func detectAndRewriteQuery(ctx context.Context, db *sql.DB, query string) (*queryRewriteResult, error) {
	tableName := extractTableName(query)
	if tableName == "" {
		// Complex query — can't auto-detect geometry columns
		return &queryRewriteResult{RewrittenQuery: query}, nil
	}

	colInfos, err := getTableColumns(ctx, db, tableName)
	if err != nil || len(colInfos) == 0 {
		return &queryRewriteResult{RewrittenQuery: query}, nil
	}

	// Determine which columns the user selected
	selectedCols := extractSelectedColumns(query)
	var columns []columnMeta
	if selectedCols == nil {
		// SELECT *
		columns = colInfos
	} else {
		colMap := make(map[string]columnMeta)
		for _, c := range colInfos {
			colMap[strings.ToUpper(c.Name)] = c
		}
		for _, sel := range selectedCols {
			if c, ok := colMap[strings.ToUpper(sel)]; ok {
				columns = append(columns, c)
			} else {
				columns = append(columns, columnMeta{Name: sel, DataType: "UNKNOWN"})
			}
		}
	}

	hasGeom := false
	for _, c := range columns {
		if c.DataType == "SDO_GEOMETRY" {
			hasGeom = true
			break
		}
	}

	if !hasGeom {
		return &queryRewriteResult{RewrittenQuery: query}, nil
	}

	// Build rewritten query
	var selectParts []string
	var geomColumns []geomColumnInfo
	var origSchema []string

	for _, c := range columns {
		origSchema = append(origSchema, c.Name)

		if c.DataType == "SDO_GEOMETRY" {
			geomCol := geomColumnInfo{
				OriginalName: c.Name,
				TableAlias:   "t_",
				FieldIndex:   len(selectParts),
			}
			geomColumns = append(geomColumns, geomCol)

			selectParts = append(selectParts,
				fmt.Sprintf("t_.%s.SDO_GTYPE AS %s__gtype", c.Name, c.Name),
				fmt.Sprintf("t_.%s.SDO_SRID AS %s__srid", c.Name, c.Name),
				fmt.Sprintf("t_.%s.SDO_POINT.X AS %s__px", c.Name, c.Name),
				fmt.Sprintf("t_.%s.SDO_POINT.Y AS %s__py", c.Name, c.Name),
				fmt.Sprintf("t_.%s.SDO_POINT.Z AS %s__pz", c.Name, c.Name),
				fmt.Sprintf("(SELECT LISTAGG(COLUMN_VALUE,',') WITHIN GROUP (ORDER BY ROWNUM) FROM TABLE(t_.%s.SDO_ELEM_INFO)) AS %s__einfo", c.Name, c.Name),
				fmt.Sprintf("(SELECT LISTAGG(TO_CHAR(COLUMN_VALUE,'TM9'),',') WITHIN GROUP (ORDER BY ROWNUM) FROM TABLE(t_.%s.SDO_ORDINATES)) AS %s__ords", c.Name, c.Name),
			)
		} else {
			selectParts = append(selectParts, fmt.Sprintf("t_.%s", quoteIfNeeded(c.Name)))
		}
	}

	rewritten := fmt.Sprintf("SELECT %s FROM %s t_", strings.Join(selectParts, ", "), tableName)

	whereClause := extractWhereClause(query)
	if whereClause != "" {
		rewritten += " WHERE " + whereClause
	}

	return &queryRewriteResult{
		RewrittenQuery: rewritten,
		GeomColumns:    geomColumns,
		OriginalSchema: origSchema,
	}, nil
}

// getTableColumns queries Oracle's data dictionary for column metadata.
func getTableColumns(ctx context.Context, db *sql.DB, tableName string) ([]columnMeta, error) {
	parts := strings.SplitN(strings.ToUpper(tableName), ".", 2)
	tblName := parts[len(parts)-1]

	rows, err := db.QueryContext(ctx,
		`SELECT column_name, data_type FROM all_tab_columns WHERE table_name = :1 ORDER BY column_id`,
		tblName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []columnMeta
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return nil, err
		}
		cols = append(cols, columnMeta{Name: name, DataType: dataType})
	}
	return cols, rows.Err()
}

// extractTableName extracts a simple table name from a SELECT query.
// Returns empty string for complex queries (joins, subqueries, etc.).
func extractTableName(query string) string {
	upper := strings.ToUpper(strings.TrimSpace(query))

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return ""
	}

	after := strings.TrimSpace(query[fromIdx+6:])
	if strings.HasPrefix(after, "(") {
		return ""
	}

	fields := strings.Fields(after)
	if len(fields) == 0 {
		return ""
	}

	tableName := strings.TrimRight(fields[0], ",")

	// Reject if there's a JOIN
	for _, f := range fields[1:] {
		u := strings.ToUpper(f)
		if u == "JOIN" || u == "," || u == "INNER" || u == "LEFT" || u == "RIGHT" || u == "CROSS" || u == "FULL" {
			return ""
		}
		if u == "WHERE" || u == "ORDER" || u == "GROUP" || u == "HAVING" || u == "FETCH" {
			break
		}
	}

	return tableName
}

// extractSelectedColumns returns column names for simple SELECT queries.
// Returns nil for SELECT * or complex expressions.
func extractSelectedColumns(query string) []string {
	upper := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(upper, "SELECT") {
		return nil
	}

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return nil
	}

	selectList := strings.TrimSpace(query[6:fromIdx])
	if selectList == "*" {
		return nil
	}

	parts := strings.Split(selectList, ",")
	var cols []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.Contains(p, "(") {
			cols = append(cols, p)
		} else {
			fields := strings.Fields(p)
			cols = append(cols, fields[0])
		}
	}
	return cols
}

// extractWhereClause extracts the WHERE clause content (without the WHERE keyword).
func extractWhereClause(query string) string {
	upper := strings.ToUpper(query)
	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		return ""
	}

	after := query[whereIdx+7:]

	for _, kw := range []string{" ORDER BY ", " GROUP BY ", " HAVING ", " FETCH "} {
		idx := strings.Index(strings.ToUpper(after), kw)
		if idx >= 0 {
			after = after[:idx]
		}
	}

	return strings.TrimSpace(after)
}

// reassembleGeometry reads the expanded geometry fields from a scan row and
// returns the WKB bytes. Fields at fieldIdx: gtype, srid, px, py, pz, einfo_str, ords_str.
func reassembleGeometry(scanVals []interface{}, fieldIdx int) ([]byte, int64, error) {
	gtype := toFloat64(scanVals[fieldIdx])
	px := toNullableFloat64(scanVals[fieldIdx+2])
	py := toNullableFloat64(scanVals[fieldIdx+3])
	pz := toNullableFloat64(scanVals[fieldIdx+4])
	einfoStr := toString(scanVals[fieldIdx+5])
	ordsStr := toString(scanVals[fieldIdx+6])

	if gtype == 0 {
		// NULL geometry
		return nil, 0, nil
	}

	geom := &SdoGeometry{
		GType: int64(gtype),
		SRID:  int64(toFloat64(scanVals[fieldIdx+1])),
	}

	if px != nil && py != nil {
		geom.Point.X = *px
		geom.Point.Y = *py
		if pz != nil {
			geom.Point.Z = *pz
		}
	}

	if einfoStr != "" {
		parts := strings.Split(einfoStr, ",")
		geom.ElemInfo = make([]int64, len(parts))
		for i, p := range parts {
			v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
			if err != nil {
				return nil, 0, fmt.Errorf("bad elem_info value %q: %w", p, err)
			}
			geom.ElemInfo[i] = v
		}
	}

	if ordsStr != "" {
		parts := strings.Split(ordsStr, ",")
		geom.Ordinates = make([]float64, len(parts))
		for i, p := range parts {
			v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil {
				return nil, 0, fmt.Errorf("bad ordinate value %q: %w", p, err)
			}
			geom.Ordinates[i] = v
		}
	}

	wkb, err := SdoToWKB(geom)
	if err != nil {
		return nil, 0, err
	}

	return wkb, int64(len(wkb)), nil
}

func toFloat64(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

func toNullableFloat64(v interface{}) *float64 {
	if v == nil {
		return nil
	}
	f := toFloat64(v)
	return &f
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

var oracleReservedWords = map[string]bool{
	"NUMBER": true, "DATE": true, "TABLE": true, "INDEX": true, "COLUMN": true,
	"SELECT": true, "FROM": true, "WHERE": true, "ORDER": true, "GROUP": true,
	"BY": true, "AND": true, "OR": true, "NOT": true, "NULL": true,
	"INSERT": true, "UPDATE": true, "DELETE": true, "CREATE": true, "DROP": true,
	"ALTER": true, "AS": true, "IN": true, "IS": true, "SET": true,
	"VALUES": true, "INTO": true, "LIKE": true, "BETWEEN": true, "JOIN": true,
	"ON": true, "ALL": true, "ANY": true, "EXISTS": true, "HAVING": true,
	"UNION": true, "MINUS": true, "INTERSECT": true, "DISTINCT": true,
	"COMMENT": true, "GRANT": true, "REVOKE": true, "CONNECT": true,
	"RESOURCE": true, "PUBLIC": true, "SYNONYM": true, "VIEW": true,
	"SEQUENCE": true, "TRIGGER": true, "PROCEDURE": true, "FUNCTION": true,
	"TYPE": true, "LEVEL": true, "SIZE": true, "START": true, "END": true,
	"LOOP": true, "RETURN": true, "EXCEPTION": true, "CURSOR": true,
	"DECLARE": true, "BEGIN": true, "OPEN": true, "CLOSE": true, "FETCH": true,
	"CHAR": true, "VARCHAR": true, "VARCHAR2": true, "INTEGER": true, "FLOAT": true,
	"LONG": true, "RAW": true, "BLOB": true, "CLOB": true, "TIMESTAMP": true,
	"INTERVAL": true, "ROWID": true, "ROWNUM": true, "SYSDATE": true,
}

func quoteIfNeeded(name string) string {
	if strings.ContainsAny(name, " .-/") || oracleReservedWords[strings.ToUpper(name)] {
		return fmt.Sprintf(`"%s"`, name)
	}
	return name
}
