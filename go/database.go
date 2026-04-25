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
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	go_ora "github.com/sijms/go-ora/v2"
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
	walletContent  string // Inline ewallet.pem content (avoids temp dir)
	dsn            string

	// Performance tuning
	poolSize      int // Max open connections (default: 8)
	ingestWorkers int // Parallel insert workers (default: 1)

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
	case OptionWalletContent:
		db.walletContent = val
	case OptionDSN:
		db.dsn = val
	case OptionPoolSize:
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			db.poolSize = n
		}
	case OptionIngestWorkers:
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			db.ingestWorkers = n
		}
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
		// If the user passed something that isn't a URL — typically a TNS
		// alias from an Autonomous Database wallet — try to resolve it
		// against tnsnames.ora in walletLocation. This lets users paste in
		// the same alias they'd use with sqlplus / sqlcl.
		if !strings.Contains(db.dsn, "://") && db.walletLocation != "" {
			if aliases, err := loadTNSAliases(db.walletLocation); err == nil {
				if entry, ok := aliases[strings.ToLower(db.dsn)]; ok {
					return assembleOracleURL(db.user, db.password, entry.Host, entry.Port, entry.Service, db.walletLocation, db.walletPassword)
				}
			}
		}
		return db.dsn
	}

	return assembleOracleURL(db.user, db.password, db.hostname, db.port, db.serviceName, db.walletLocation, db.walletPassword)
}

// assembleOracleURL builds the oracle://user:pass@host:port/service URL with
// the same query-parameter conventions used historically by buildDSN.
func assembleOracleURL(user, password, host, port, service, walletLocation, walletPassword string) string {
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
		url.PathEscape(user),
		url.PathEscape(password),
		host,
		port,
		service,
	)
	params := url.Values{}
	params.Set("PREFETCH_ROWS", "10000")
	if walletLocation != "" {
		// Wallet implies TCPS for Autonomous Database; switch SSL on.
		// SSL VERIFY=false matches the wallet's mTLS chain rather than the
		// hostname, which is what ATP wallets are designed for.
		params.Set("WALLET", walletLocation)
		params.Set("SSL", "enable")
		params.Set("SSL VERIFY", "false")
	}
	if walletPassword != "" {
		params.Set("WALLET PASSWORD", walletPassword)
	}
	if len(params) > 0 {
		dsn += "?" + params.Encode()
	}
	return dsn
}

// getPool returns the shared connection pool, creating it on first call.
func (db *databaseImpl) getPool(ctx context.Context) (*sql.DB, error) {
	db.poolOnce.Do(func() {
		var pool *sql.DB
		var err error

		if db.walletContent != "" {
			// Use Connector API for in-memory wallet (avoids temp files).
			// Wallet content is base64-encoded cwallet.sso binary data.
			walletBytes, decErr := base64.StdEncoding.DecodeString(db.walletContent)
			if decErr != nil {
				db.poolErr = fmt.Errorf("failed to decode wallet content (expected base64): %w", decErr)
				return
			}
			dsn := db.buildDSN()
			connector := go_ora.NewConnector(dsn).(*go_ora.OracleConnector)
			if walletErr := connector.WithWallet(bytes.NewReader(walletBytes)); walletErr != nil {
				db.poolErr = fmt.Errorf("failed to load wallet content: %w", walletErr)
				return
			}
			pool = sql.OpenDB(connector)
		} else {
			dsn := db.buildDSN()
			pool, err = sql.Open("oracle", dsn)
			if err != nil {
				db.poolErr = fmt.Errorf("failed to open Oracle connection pool: %w", err)
				return
			}
		}

		if err := pool.PingContext(ctx); err != nil {
			pool.Close()
			db.poolErr = fmt.Errorf("failed to ping Oracle: %w", err)
			return
		}

		// Allow parallel inserts — set pool size for concurrent batch operations
		poolSize := db.poolSize
		if poolSize <= 0 {
			poolSize = 8
		}
		pool.SetMaxOpenConns(poolSize)
		pool.SetMaxIdleConns(poolSize)

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
		ingestWorkers:      db.ingestWorkers,
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
