# adbc-driver-oracle

An [ADBC](https://arrow.apache.org/adbc/) driver for Oracle Database, written in Go.

**Key feature:** Client-side SDO_GEOMETRY to WKB conversion via direct UDT wire decode — no server-side `Get_WKB()` or `SDO_UTIL.TO_WKBGEOMETRY()` needed.

## Architecture

```
Oracle DB ──(TNS wire)──> go-ora (UDT decode) ──> SdoGeometry struct ──> WKB ──> geoarrow.wkb Arrow column
                                │                                                         │
                                └── pure Go, no Oracle Instant Client            DuckDB adbc_scanner
```

## Features

- **Pure Go** — no Oracle Instant Client or CGO dependency (uses [go-ora](https://github.com/sijms/go-ora))
- **Oracle Autonomous Database** — wallet/TLS authentication supported
- **SDO_GEOMETRY → geoarrow.wkb** — points, lines, polygons, multi-geometries decoded from the wire protocol
- **SRID detection** — extracted from the geometry's wire data, embedded as PROJJSON CRS in Arrow metadata
- **ADBC metadata** — GetObjects, GetTableSchema, GetTableTypes, catalog/schema navigation
- **Transactions** — Commit/Rollback support

## Performance

Tested against Oracle Autonomous DB (Ashburn), FIRES_WORLDWIDE table (333K rows, 15 columns):

| Metric | Value |
|--------|-------|
| `SELECT *` with geometry | **60,000+ rows/sec** |
| Baseline (oracledb + WKT) | 11,200 rows/sec |
| Speedup | **5.4x** |

## Usage

### CLI test harness

```bash
go build -o oracle-adbc-test ./cmd/

# Direct connection
./oracle-adbc-test -dsn "oracle://user:pass@host:1521/service" -query "SELECT * FROM my_table"

# Oracle Autonomous DB with wallet
./oracle-adbc-test \
  -dsn "oracle://ADMIN:pass@adb.us-ashburn-1.oraclecloud.com:1522/service?ssl=enable&ssl verify=false&wallet=/path/to/wallet" \
  -query "SELECT * FROM my_table"

# Benchmark mode
./oracle-adbc-test -dsn "..." -benchmark -query "SELECT * FROM large_table"
```

### As a Go library

```go
import (
    oracle "github.com/jatorre/adbc-driver-oracle/go"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

drv := oracle.NewDriver(memory.DefaultAllocator)
db, _ := drv.NewDatabase(map[string]string{
    oracle.OptionDSN: "oracle://user:pass@host:1521/service",
})
cnxn, _ := db.Open(ctx)
stmt, _ := cnxn.NewStatement()
stmt.SetSqlQuery("SELECT * FROM my_table")
reader, _, _ := stmt.ExecuteQuery(ctx)
// reader is an Arrow RecordReader with geoarrow.wkb geometry columns
```

## Supported Oracle Types

| Oracle Type | Arrow Type | Notes |
|------------|------------|-------|
| NUMBER (integer, p<=18) | Int64 | |
| NUMBER (integer, p>18) | String | Preserves full precision |
| NUMBER (decimal), FLOAT, BINARY_FLOAT/DOUBLE | Float64 | |
| VARCHAR2, CHAR, CLOB, NVARCHAR2, NCHAR, NCLOB | String | |
| DATE | Timestamp (us, no TZ) | Oracle DATE is wall-clock time |
| TIMESTAMP | Timestamp (us, no TZ) | |
| TIMESTAMP WITH TIME ZONE | Timestamp (us, UTC) | |
| TIMESTAMP WITH LOCAL TIME ZONE | Timestamp (us, UTC) | |
| INTERVAL YEAR TO MONTH | String | e.g. `+03-06` |
| INTERVAL DAY TO SECOND | String | e.g. `+05 04:03:02.000000` |
| RAW, BLOB | Binary | |
| BOOLEAN (Oracle 23c+) | Boolean | |
| SDO_GEOMETRY | Binary (geoarrow.wkb) | With SRID as PROJJSON CRS |

## Bulk Ingest (Arrow → Oracle)

`adbc.ingest.target_table` + `Bind`/`BindStream` creates the destination table
from the Arrow schema:

| Arrow Type | Oracle Type | Notes |
|------------|-------------|-------|
| Int8..Int64, Uint8..Uint32 | NUMBER(19) | |
| Uint64 | NUMBER(20) | Full uint64 range |
| Float16/32 | BINARY_FLOAT | |
| Float64 | BINARY_DOUBLE | |
| Decimal128/256 | NUMBER(p,s) | p capped at 38 |
| String, LargeString | VARCHAR2(4000) or CLOB | See string sizing below |
| Binary, LargeBinary, FixedSizeBinary | BLOB | |
| Boolean | NUMBER(1) | Bound as 0/1 |
| Timestamp (no TZ) | TIMESTAMP(6) — TIMESTAMP(9) for ns | Bound as native TIMESTAMP (wall clock preserved) |
| Timestamp (with TZ) | TIMESTAMP(6/9) WITH TIME ZONE | Bound as native TSTZ (instant preserved) |
| Date32/Date64 | DATE | |
| Binary + `geoarrow.wkb` metadata | MDSYS.SDO_GEOMETRY | Client-side WKB → SDO UDT |
| Other (Time, Interval, …) | VARCHAR2(4000) | Textual form |

### String column sizing

Oracle `VARCHAR2` holds at most 4000 bytes; wider values raise `ORA-12899`.
By default (`auto`) the driver scans the bound data and creates `CLOB` for any
string column with a value over 4000 bytes. A single bound batch (`Bind`) is
scanned in full; a bound stream (`BindStream`) is scanned up to
`oracle.ingest.type_scan_limit_bytes` (64 MiB, may overshoot by one batch) —
the scanned records are buffered and inserted first, then the rest streams
through. With the scan disabled (`0`), `LargeString` columns map to `CLOB` and
regular strings to `VARCHAR2(4000)`. Statement options:

| Option | Values | Default |
|--------|--------|---------|
| `oracle.ingest.string_type` | `auto`, `varchar2`, `clob` | `auto` |
| `oracle.ingest.clob_columns` | comma-separated column names forced to CLOB | — |
| `oracle.ingest.type_scan_limit_bytes` | stream bytes the auto scan may buffer; `0` disables the scan | `67108864` |

If wide values appear only deep in a large stream (beyond the scan window),
force those columns with `oracle.ingest.clob_columns` — the ORA-12899 error
message names this option when the auto scan guesses wrong.

## How Geometry Works

1. On connection open, the driver registers `MDSYS.SDO_GEOMETRY` and its sub-types as go-ora UDTs
2. go-ora decodes Oracle's binary TNS wire format directly into `SdoGeometry` Go structs
3. The driver converts `SdoGeometry` → WKB (Well-Known Binary) client-side
4. The result is an Arrow `binary` column with `geoarrow.wkb` extension metadata and PROJJSON CRS

Supported geometry types: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, optimized rectangles, and arc densification.

## Development

```bash
# Run tests
go test -v ./go/

# Build
go build -o oracle-adbc-test ./cmd/
```

## Credits

Developed by [CARTO](https://carto.com/).

## License

Copyright 2025 CARTO. Licensed under the Apache License 2.0.
