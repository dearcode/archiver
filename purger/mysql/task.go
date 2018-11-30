package mysql

import (
	"context"
	"database/sql"

	"github.com/dearcode/archiver/purger"
	"github.com/dearcode/crab/log"
	"github.com/dearcode/crab/orm"
	"github.com/juju/errors"
)

type mysqlPurgeTask struct {
	dbc      *sql.DB
	session  string
	ctx      context.Context
	stmt     *sql.Stmt
	argvChan chan string
}

const (
	maxDBTimeout = 60
)

func (t *mysqlPurgeTask) String() string {
	return t.session
}

func (t *mysqlPurgeTask) end() {
	if t.stmt != nil {
		t.stmt.Close()
		t.stmt = nil
	}

	if t.dbc != nil {
		t.dbc.Close()
		t.dbc = nil
	}

	log.Infof("%v end")
}

func (t *mysqlPurgeTask) Start(ctx context.Context, ip string, port int, user, password, dbName, sqlStmt string) (chan string, error) {
	if v := ctx.Value("session"); v != nil {
		t.session = v.(string)
	}

	db := orm.NewDB(ip, port, dbName, user, password, "utf8", maxDBTimeout)
	dbc, err := db.GetConnection()
	if err != nil {
		return nil, errors.Trace(err)
	}

	stmt, err := dbc.Prepare(sqlStmt)
	if err != nil {
		dbc.Close()
		return nil, errors.Trace(err)
	}

	t.dbc = dbc
	t.ctx = ctx
	t.stmt = stmt
	t.argvChan = make(chan string)

	go t.run()

	return t.argvChan, nil
}

func (t *mysqlPurgeTask) run() {
	defer t.end()

	for {
		select {
		case <-t.ctx.Done():
			log.Errorf("%v done", t)
			return
		case argv := <-t.argvChan:
            log.Debugf("%v argv:%v", t, argv)
            /*
			c, err := t.stmt.Exec(argv)
			if err != nil {
				log.Errorf("%v exec argv:%+v error:%v", t, argv, err)
				return
			}
			ra, err := c.RowsAffected()
			if err != nil {
				log.Errorf("%v RowsAffected error:%v", t, err)
				return
			}
			log.Debugf("%v RowsAffected:%v", t, ra)
            */
		}
	}
}

func (t mysqlPurgeTask) Name() string {
	return "mysql"
}

func init() {
	purger.Register(&mysqlPurgeTask{})
}
