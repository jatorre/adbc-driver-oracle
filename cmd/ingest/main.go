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

// Ingest Parquet files into Oracle via ADBC bulk ingest.
//
// Usage:
//
//	go run ./cmd/ingest -dsn "oracle://..." -table MY_TABLE -file data.parquet
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	oracle "github.com/jatorre/adbc-driver-oracle/go"
)

func main() {
	dsn := flag.String("dsn", "", "Oracle DSN")
	tableName := flag.String("table", "", "Target Oracle table name")
	filePath := flag.String("file", "", "Parquet file to ingest")
	mode := flag.String("mode", "create", "Ingest mode: create, append, replace, create_append")
	batchSize := flag.Int("batch", 10000, "Rows per batch")
	flag.Parse()

	if *dsn == "" || *tableName == "" || *filePath == "" {
		fmt.Fprintln(os.Stderr, "Usage: ingest -dsn <oracle-dsn> -table <table> -file <parquet-file>")
		os.Exit(1)
	}

	// Normalize mode names to ADBC constants
	modeMap := map[string]string{
		"create":        adbc.OptionValueIngestModeCreate,
		"append":        adbc.OptionValueIngestModeAppend,
		"replace":       adbc.OptionValueIngestModeReplace,
		"create_append": adbc.OptionValueIngestModeCreateAppend,
	}
	if m, ok := modeMap[*mode]; ok {
		*mode = m
	}

	ctx := context.Background()
	alloc := memory.DefaultAllocator

	// Open Parquet file
	f, err := os.Open(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Open file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	pf, err := file.NewParquetReader(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Parquet reader: %v\n", err)
		os.Exit(1)
	}
	defer pf.Close()

	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: int64(*batchSize)}, alloc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Arrow reader: %v\n", err)
		os.Exit(1)
	}

	schema, err := arrowReader.Schema()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Schema: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Schema: %s\n", schema)
	fmt.Printf("Total rows: %d\n", pf.NumRows())

	// Get a streaming record reader
	recReader, err := arrowReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetRecordReader: %v\n", err)
		os.Exit(1)
	}
	defer recReader.Release()

	// Connect to Oracle
	drv := oracle.NewDriver(alloc)
	db, err := drv.NewDatabase(map[string]string{oracle.OptionDSN: *dsn})
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

	// Ingest batches
	start := time.Now()
	var totalRows int64
	batchNum := 0
	currentMode := *mode

	for recReader.Next() {
		rec := recReader.Record()

		stmt, err := cnxn.NewStatement()
		if err != nil {
			fmt.Fprintf(os.Stderr, "NewStatement: %v\n", err)
			os.Exit(1)
		}

		if gs, ok := stmt.(adbc.GetSetOptions); ok {
			gs.SetOption(adbc.OptionKeyIngestTargetTable, *tableName)
			gs.SetOption(adbc.OptionKeyIngestMode, currentMode)
		}

		if err := stmt.Bind(ctx, rec); err != nil {
			fmt.Fprintf(os.Stderr, "Bind: %v\n", err)
			os.Exit(1)
		}

		affected, err := stmt.ExecuteUpdate(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ExecuteUpdate batch %d: %v\n", batchNum, err)
			os.Exit(1)
		}
		stmt.Close()

		totalRows += affected
		batchNum++
		elapsed := time.Since(start)
		fmt.Printf("  Batch %d: +%d rows (%d total, %.0f rows/sec)\n",
			batchNum, affected, totalRows, float64(totalRows)/elapsed.Seconds())

		// After first batch, switch to append mode
		currentMode = adbc.OptionValueIngestModeAppend
	}

	if err := recReader.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Reader error: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	fmt.Printf("\nDone: %d rows in %s (%.0f rows/sec)\n",
		totalRows, elapsed.Round(time.Millisecond), float64(totalRows)/elapsed.Seconds())
}
