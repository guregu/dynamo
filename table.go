package dynamo

// Table is a DynamoDB table.
type Table struct {
	name string
	db   *DB
}

// Name returns this table's name.
func (t Table) Name() string {
	return t.name
}

func (db *DB) Table(name string) Table {
	return Table{
		name: name,
		db:   db,
	}
}
