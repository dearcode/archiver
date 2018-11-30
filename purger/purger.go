package purger

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/dearcode/crab/log"
)

var (
	mu      sync.Mutex
	modules = make(map[string]PurgerModule)
)

//PurgerModule 清理接口.
type PurgerModule interface {
	Name() string
	Start(ctx context.Context, ip string, port int, user, password, db, stmt string) (chan string, error)
}

//Register 添加模块.
func Register(m PurgerModule) {
	mu.Lock()
	defer mu.Unlock()
	modules[m.Name()] = m
	log.Debugf("new module %s", m.Name())
}

//New 创建purger对象.
func New(name string) (PurgerModule, error) {
	mu.Lock()
	defer mu.Unlock()

	m, ok := modules[name]
	if !ok {
		return nil, fmt.Errorf("module %s not found", name)
	}

	return reflect.New(reflect.TypeOf(m).Elem()).Interface().(PurgerModule), nil
}
