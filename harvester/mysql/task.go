package mysql

import (
	"database/sql"
	"strings"

	"github.com/dearcode/crab/orm"
	"github.com/juju/errors"

	"github.com/dearcode/archiver/meta"
)

type mysqlTask struct {
	ip         string
	port       int
	user       string
	password   string
	db         string
	table      string
	where      string
	dbc        *sql.DB
	cols       []meta.ColumnDef
	PrimaryKey string
}

func New(ip string, port int, user, password, db, table, where string) *mysqlTask {
	return &mysqlTask{
		ip:       ip,
		port:     port,
		user:     user,
		password: password,
		db:       db,
		table:    table,
		where:    where,
	}
}

func (t *mysqlTask) tableDesc(dbc *sql.DB, table string) error {
	rows, err := dbc.Query("desc " + table)
	if err != nil {
		return errors.Trace(err)
	}

	var cols []meta.ColumnDef
	for rows.Next() {
		var col meta.ColumnDef
		if err = rows.Scan(&col.Field, &col.Type, &col.Null, &col.Key, &col.Default, &col.Extra); err != nil {
			return errors.Trace(err)
		}
		cols = append(cols, col)
	}

	t.cols = cols

	return nil
}

func (t *mysqlTask) dumpSQL() error {

}

func (t *mysqlTask) Start() error {
	db := orm.NewDB(t.ip, t.port, t.db, t.user, t.password, "utf8", 60)
	dbc, err := db.GetConnection()
	if err != nil {
		return errors.Trace(err)
	}
	t.dbc = dbc

	if err = tableDesc(dbc); err != nil {
		return errors.Trace(err)
	}

	for _, c := range t.cols {
		if strings.EqulFold(c.Key == "PRI") {
			t.PrimaryKey = c.Field
			break
		}
	}

	if t.PrimaryKey == "" {
		return errors.New("table primary key not found")
	}

}
