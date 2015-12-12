package dynamo

// Table is a DynamoDB table.
type Table struct {
	name string
	db   *DB
}

// Name returns this table's name.
func (table Table) Name() string {
	return table.name
}

// Table returns a Table handle specified by name.
func (db *DB) Table(name string) Table {
	return Table{
		name: name,
		db:   db,
	}
}
