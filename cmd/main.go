// Package main provides a standalone test harness for the Oracle ADBC driver.
//
// Usage:
//
//	go run ./cmd/ -dsn "oracle://user:pass@host:port/service"
//
// To build as a C-shared library for adbc_scanner, the driver needs the
// ADBC CGO bridge (generated from apache/arrow-adbc/go/adbc/pkg/_tmpl/).
// See BUILD.md for instructions.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow/memory"

	oracle "github.com/jatorre/adbc-driver-oracle/go"
)

func main() {
	dsn := flag.String("dsn", "", "Oracle DSN (oracle://user:pass@host:port/service)")
	query := flag.String("query", "SELECT 1 FROM DUAL", "SQL query to execute")
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "Usage: oracle-adbc-test -dsn <oracle-dsn> [-query <sql>]")
		os.Exit(1)
	}

	ctx := context.Background()
	drv := oracle.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		oracle.OptionDSN: *dsn,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewDatabase: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Open: %v\n", err)
		os.Exit(1)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewStatement: %v\n", err)
		os.Exit(1)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(*query); err != nil {
		fmt.Fprintf(os.Stderr, "SetSqlQuery: %v\n", err)
		os.Exit(1)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ExecuteQuery: %v\n", err)
		os.Exit(1)
	}
	defer reader.Release()

	schema := reader.Schema()
	fmt.Printf("Schema: %s\n", schema)

	totalRows := 0
	for reader.Next() {
		rec := reader.Record()
		totalRows += int(rec.NumRows())
		if totalRows <= 10 {
			fmt.Println(rec)
		}
	}
	if err := reader.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Reader error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nTotal rows: %d\n", totalRows)
}
