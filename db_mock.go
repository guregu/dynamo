package dynamo

func NewMockDB() *DB {
	return &DB{
		isMock: true,
	}
}
