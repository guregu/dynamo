package dynamo

type Table struct {
	Name string
	db   *DB
}

func (db *DB) Table(name string) Table {
	return Table{
		Name: name,
		db:   db,
	}
}
