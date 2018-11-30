package harvester

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"

	"github.com/dearcode/crab/log"

	"github.com/dearcode/archiver/meta"
)

var (
	mu      sync.Mutex
	modules = make(map[string]HarvesterModule)
)

//HarvesterModule 收集器接口.
type HarvesterModule interface {
	Name() string
	Start(ctx context.Context, dns, table, where string, limit int) (<-chan []sql.NullString, error)
	TableDef() []meta.ColumnDef
	PrimaryKey() string
}

//Register 添加模块.
func Register(m HarvesterModule) {
	mu.Lock()
	defer mu.Unlock()
	modules[m.Name()] = m
	log.Debugf("new module %s", m.Name())
}

//New 创建指定模块对象.
func New(name string) (HarvesterModule, error) {
	mu.Lock()
	defer mu.Unlock()

	m, ok := modules[name]
	if !ok {
		return nil, fmt.Errorf("module %s not found", name)
	}

	return reflect.New(reflect.TypeOf(m).Elem()).Interface().(HarvesterModule), nil
}
