package mysql

import (
	"bytes"
	"context"
	"testing"

	"github.com/dearcode/crab/uuid"
	"github.com/juju/errors"
)

func TestDump(t *testing.T) {
	task := &mysqlTask{}
	rc, err := task.Start(context.Background(), uuid.String(), "192.168.180.104", 3306, "dbfree", "dbfree", "test", "userinfo", "id > 1")
	if err != nil {
		t.Fatalf(errors.ErrorStack(err))
	}

	for r := range rc {
        bs := bytes.NewBufferString("")

        for _, s := range r {
            bs.WriteString(s.String)
            bs.WriteString("\t")
        }

		t.Logf("%v", bs.String())
	}
}
