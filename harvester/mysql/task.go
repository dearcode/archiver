package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dearcode/archiver/harvester"
	"github.com/dearcode/crab/log"
	"github.com/juju/errors"

	"github.com/dearcode/archiver/meta"
)

type mysqlTask struct {
	session       string
	table         string
	where         string
	fields        []string
	fieldKey      string
	dbc           *sql.DB
	cols          []meta.ColumnDef
	lastKey       string
	primaryKey    string
	primaryKeyIdx int
	eventChan     chan struct{}
	rowChan       chan []sql.NullString
	ctx           context.Context
	err           error
	limit         int
}

func (t mysqlTask) String() string {
	return t.session
}

//ColumnNames 所有列名.
func (t *mysqlTask) ColumnNames() []string {
	return t.fields
}

//ColumnKey 所有列名拼接.
func (t *mysqlTask) ColumnKey() string {
	return t.fieldKey
}

//PrimaryKey 主键.
func (t *mysqlTask) PrimaryKey() string {
	return t.primaryKey
}

//TableDef 表结构.
func (t *mysqlTask) TableDef() []meta.ColumnDef {
	return t.cols
}

func (t *mysqlTask) tableDesc(dbc *sql.DB) error {
	rows, err := dbc.Query("desc " + t.table)
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
		t.fields = append(t.fields, col.Field)
	}

	t.cols = cols
	t.fieldKey = strings.Join(t.fields, ",")

	return nil
}

func (t *mysqlTask) row2NullString(rows *sql.Rows) ([]sql.NullString, error) {
	row := make([]sql.NullString, len(t.cols))
	cols := make([]interface{}, len(t.cols))
	for i := range cols {
		cols[i] = &row[i]
	}

	if err := rows.Scan(cols...); err != nil {
		return nil, err
	}

	return row, nil
}

func (t *mysqlTask) row2sql(rows *sql.Rows) (string, error) {
	cols := make([]interface{}, len(t.cols))
	for i := range cols {
		cols[i] = &sql.NullString{}
	}

	if err := rows.Scan(cols...); err != nil {
		return "", err
	}

	bs := bytes.NewBufferString("insert into ")
	bs.WriteString(t.table)
	bs.WriteString("(")
	bs.WriteString(t.fieldKey)
	bs.WriteString(") values (")

	for _, c := range cols {
		col := c.(*sql.NullString)
		val := "NULL"
		//TODO blob转换
		if col.Valid {
			val = fmt.Sprintf("'%v'", col.String)
		}
		bs.WriteString(val)
		bs.WriteString(",")
	}
	bs.Truncate(bs.Len() - 1)
	bs.WriteString(")")
	return bs.String(), nil
}

func (t *mysqlTask) queryPrepare(dbc *sql.DB) (*sql.Stmt, error) {
	bs := bytes.NewBufferString("select ")
	bs.WriteString(t.fieldKey)
	bs.WriteString(" from ")
	bs.WriteString(t.table)
	if t.where != "" {
		bs.WriteString(" where ")
		bs.WriteString(t.where)
		bs.WriteString(" and ")
		bs.WriteString(t.primaryKey)
		bs.WriteString(" > ? limit ?")
	} else {
		bs.WriteString(" where ")
		bs.WriteString(t.primaryKey)
		bs.WriteString(" > ? limit ?")
	}

	stmt := bs.String()
	log.Debugf("%v sql:%v", t, stmt)

	return dbc.Prepare(stmt)
}

func (t *mysqlTask) batchQuery(stmt *sql.Stmt, lastKey string) (string, error) {
	rows, err := stmt.Query(lastKey, t.limit)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-t.ctx.Done():
			return lastKey, nil
		default:
		}

		row, err := t.row2NullString(rows)
		if err != nil {
			return "", errors.Trace(err)
		}

		lastKey = row[t.primaryKeyIdx].String
		t.rowChan <- row
	}

	return lastKey, nil
}

func (t *mysqlTask) dumpSQL() {
	defer close(t.rowChan)

	stmt, err := t.queryPrepare(t.dbc)
	if err != nil {
		t.err = err
		log.Errorf("%v queryPrepare error:%v", t, err)
		return
	}
	defer stmt.Close()

	var lastKey string
	for {
		select {
		case <-t.ctx.Done():
			log.Infof("%v done", t)
			return
		case <-t.eventChan:
		}
		if lastKey, err = t.batchQuery(stmt, lastKey); err != nil {
			log.Errorf("%v batchQuery error:%v", t, errors.ErrorStack(err))
			return
		}
	}

}

func (t *mysqlTask) Start(ctx context.Context, dsn string, table, where string, limit int) (<-chan []sql.NullString, error) {
	t.ctx = ctx
	t.table = table
	t.where = where

	if v := ctx.Value("session"); v != nil {
		t.session = v.(string)
	}

	dbc, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	t.dbc = dbc

	if err = t.tableDesc(dbc); err != nil {
		return nil, errors.Trace(err)
	}

	for i, c := range t.cols {
		if strings.EqualFold(c.Key, "PRI") {
			t.primaryKey = c.Field
			t.primaryKeyIdx = i
			break
		}
	}

	if t.primaryKey == "" {
		return nil, errors.New("table primary key not found")
	}

	t.rowChan = make(chan []sql.NullString)

	go t.dumpSQL()

	return t.rowChan, nil
}

func (t mysqlTask) Name() string {
	return "mysql"
}

func init() {
	harvester.Register(&mysqlTask{})
}
