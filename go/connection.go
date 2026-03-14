package oracle

import (
	"database/sql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
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

func (c *connectionImpl) Close() error {
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}
