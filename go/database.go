package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

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

	// Build oracle://user:pass@host:port/service_name?params
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
		url.PathEscape(db.user),
		url.PathEscape(db.password),
		db.hostname,
		db.port,
		db.serviceName,
	)

	params := url.Values{}
	// Default prefetch for good throughput (go-ora default is too low)
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

func (db *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	dsn := db.buildDSN()

	sqlDB, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, db.ErrorHelper.Errorf(adbc.StatusIO, "failed to open Oracle connection: %s", err)
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, db.ErrorHelper.Errorf(adbc.StatusIO, "failed to ping Oracle: %s", err)
	}

	// Register SDO_GEOMETRY UDT so go-ora can decode geometry columns directly
	if err := RegisterSDOTypes(sqlDB); err != nil {
		// Non-fatal: geometry columns will fall back to query rewriting
		db.Logger.Warn("SDO_GEOMETRY UDT registration failed, using query rewriting fallback", "error", err)
	}

	cnxnBase := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)
	cnxn := &connectionImpl{
		ConnectionImplBase: cnxnBase,
		db:                 sqlDB,
	}

	return driverbase.NewConnectionBuilder(cnxn).
		WithDbObjectsEnumerator(cnxn).
		WithTableTypeLister(cnxn).
		WithCurrentNamespacer(cnxn).
		Connection(), nil
}

func (db *databaseImpl) Close() error {
	return db.DatabaseImplBase.Close()
}
