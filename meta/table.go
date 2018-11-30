package meta

import (
	"database/sql"
)

type ColumnDef struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default sql.NullString
	Extra   string
}
