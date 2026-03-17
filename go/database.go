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
	"database/sql"
	"fmt"
	"net/url"
	"sync"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	_ "github.com/sijms/go-ora/v2"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	hostname       string
	port           string
	serviceName    string
	user           string
	password       string
	walletLocation string
	walletPassword string
	dsn            string

	// Connection pool — shared across all Open() calls.
	// sql.DB is already a pool internally; we create it once and reuse.
	pool     *sql.DB
	poolOnce sync.Once
	poolErr  error
}

func (db *databaseImpl) SetOption(key string, val string) error {
	switch key {
	case OptionServerHostname:
		db.hostname = val
	case OptionPort:
		db.port = val
	case OptionServiceName:
		db.serviceName = val
	case OptionUser, adbc.OptionKeyUsername:
		db.user = val
	case OptionPassword, adbc.OptionKeyPassword:
		db.password = val
	case OptionWalletLocation:
		db.walletLocation = val
	case OptionWalletPassword:
		db.walletPassword = val
	case OptionDSN:
		db.dsn = val
	default:
		return db.DatabaseImplBase.SetOption(key, val)
	}
	return nil
}

func (db *databaseImpl) SetOptions(options map[string]string) error {
	for key, val := range options {
		if err := db.SetOption(key, val); err != nil {
			return err
		}
	}
	return nil
}

func (db *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionServerHostname:
		return db.hostname, nil
	case OptionPort:
		return db.port, nil
	case OptionServiceName:
		return db.serviceName, nil
	case OptionUser:
		return db.user, nil
	case OptionWalletLocation:
		return db.walletLocation, nil
	default:
		return db.DatabaseImplBase.GetOption(key)
	}
}

func (db *databaseImpl) buildDSN() string {
	if db.dsn != "" {
		return db.dsn
	}

	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
		url.PathEscape(db.user),
		url.PathEscape(db.password),
		db.hostname,
		db.port,
		db.serviceName,
	)

	params := url.Values{}
	params.Set("PREFETCH_ROWS", "10000")
	if db.walletLocation != "" {
		params.Set("WALLET", db.walletLocation)
	}
	if db.walletPassword != "" {
		params.Set("WALLET PASSWORD", db.walletPassword)
	}
	if len(params) > 0 {
		dsn += "?" + params.Encode()
	}

	return dsn
}

// getPool returns the shared connection pool, creating it on first call.
func (db *databaseImpl) getPool(ctx context.Context) (*sql.DB, error) {
	db.poolOnce.Do(func() {
		dsn := db.buildDSN()
		pool, err := sql.Open("oracle", dsn)
		if err != nil {
			db.poolErr = fmt.Errorf("failed to open Oracle connection pool: %w", err)
			return
		}

		if err := pool.PingContext(ctx); err != nil {
			pool.Close()
			db.poolErr = fmt.Errorf("failed to ping Oracle: %w", err)
			return
		}

		// Allow parallel inserts — set pool size for concurrent batch operations
		pool.SetMaxOpenConns(8)
		pool.SetMaxIdleConns(8)

		// Register SDO_GEOMETRY UDT so go-ora can decode geometry columns
		if err := RegisterSDOTypes(pool); err != nil {
			db.Logger.Warn("SDO_GEOMETRY UDT registration failed", "error", err)
		}

		db.pool = pool
	})
	return db.pool, db.poolErr
}

func (db *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	pool, err := db.getPool(ctx)
	if err != nil {
		return nil, db.ErrorHelper.Errorf(adbc.StatusIO, "%s", err)
	}

	cnxnBase := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)
	cnxn := &connectionImpl{
		ConnectionImplBase: cnxnBase,
		db:                 pool,
		ownsDB:             false, // pool is owned by the database, not the connection
	}

	return driverbase.NewConnectionBuilder(cnxn).
		WithDbObjectsEnumerator(cnxn).
		WithTableTypeLister(cnxn).
		WithCurrentNamespacer(cnxn).
		Connection(), nil
}

func (db *databaseImpl) Close() error {
	if db.pool != nil {
		err := db.pool.Close()
		db.pool = nil
		return err
	}
	return db.DatabaseImplBase.Close()
}
