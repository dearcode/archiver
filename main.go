package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"

	"github.com/dearcode/crab/log"
	"github.com/dearcode/crab/uuid"

	"github.com/dearcode/archiver/harvester"
	_ "github.com/dearcode/archiver/harvester/mysql"
	"github.com/dearcode/archiver/purger"
	_ "github.com/dearcode/archiver/purger/mysql"
)

var (
	srcType = flag.String("t", "mysql", "source db type, default is mysql")
	source  = flag.String("source", "", "DSN specifying the table to archive from (required), username:password@tcp(address)/dbname")
	dest    = flag.String("dest", "", "DSN specifying the table to archive, username:password@tcp(address)/dbname")
	table   = flag.String("T", "", "table name")
	where   = flag.String("W", "", "where clause to limit which rows to archiv")
	purge   = flag.Bool("-purge", false, "purge data")
	limit   = flag.Int("-limit", 1000, "select limit")
)

func main() {
	flag.StringVar(where, "-where", "", "where clause to limit which rows to archiv")
	flag.StringVar(table, "-table", "", "table name")

	flag.Parse()

	ctx := context.Background()
	session := uuid.String()
	ctx = context.WithValue(ctx, "session", session)

	h, err := harvester.New(*srcType)
	if err != nil {
		log.Fatalf("%v harvester New error:%v", session, err)
	}

	p, err := purger.New(*srcType)
	if err != nil {
		log.Fatalf("%v purger New error:%v", session, err)
	}

	rows, err := h.Start(ctx, *srouce, *table, *where, *limit)
	if err != nil {
		log.Fatalf("%v harvester Start error:%v", session, err)
	}

	stmt := fmt.Sprintf("delete from %s where %s = ?", *table, h.PrimaryKey())

	pc, err := p.Start(ctx, *host, *port, *user, *password, *db, stmt)
	if err != nil {
		log.Fatalf("%v purger Start error:%v", session, err)
	}

	idx := 0
	for i, c := range h.TableDef() {
		if c.Key == "PRI" {
			idx = i
		}
	}

	for row := range rows {
		pc <- row[idx].String
	}

}
