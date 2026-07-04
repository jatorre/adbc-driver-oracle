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
	"context"
	"runtime/debug"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	OptionServerHostname = "oracle.server_hostname"
	OptionPort           = "oracle.port"
	OptionServiceName    = "oracle.service_name"
	OptionUser           = "oracle.user"
	OptionPassword       = "oracle.password"
	OptionWalletLocation = "oracle.wallet_location"
	OptionWalletPassword = "oracle.wallet_password"
	OptionWalletContent  = "oracle.wallet_content" // Inline ewallet.pem content (avoids temp file)
	OptionDSN            = "oracle.dsn"
	OptionPoolSize       = "oracle.pool_size"      // Max open connections (default: 8)
	OptionIngestWorkers  = "oracle.ingest_workers" // Parallel insert workers (default: 1)

	// Statement options controlling how Arrow string columns are typed in
	// ingest CREATE TABLE DDL. VARCHAR2 caps at 4000 bytes; values wider than
	// that need CLOB or the insert fails with ORA-12899.
	//
	// string_type: "auto" (default) scans the head of the bound data and uses
	// CLOB for any string column with a value over 4000 bytes; "varchar2" and
	// "clob" force the respective type for all string columns.
	OptionIngestStringType = "oracle.ingest.string_type"
	// clob_columns: comma-separated (case-insensitive) column names always
	// created as CLOB, regardless of string_type or scan results. Use this
	// when wide values only appear deep in the stream where the auto scan
	// cannot see them.
	OptionIngestClobColumns = "oracle.ingest.clob_columns"
	// type_scan_limit_bytes: how much of the bound stream "auto" may buffer
	// while sizing string columns (default 64 MiB).
	OptionIngestTypeScanLimit = "oracle.ingest.type_scan_limit_bytes"

	DefaultPort = "1521"
)

var infoVendorVersion string

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch dep.Path {
			case "github.com/sijms/go-ora/v2":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Oracle")
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoVendorSql: true,
	})
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}

	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		port:             DefaultPort,
		poolSize:         8,
		ingestWorkers:    1,
	}

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
