package sync

import (
	"gorm.io/gorm"
)

type CollectionOptions struct {
	Skip bool
	// Add other options as needed
}

type ReadableDb interface {
	Describe() string
	Connect() error
	GetCollections() ([]ReadableCollection, error)
	Close() error
}

type ReadWriteDb interface {
	ReadableDb
	UpsertCollection(name string, collectionType string) error
	DeleteCollection(name string) error
}

type ReadableCollection struct {
	Name string
	Type string
}

// Define a struct that wraps gorm.DB
type GormDb struct {
	*gorm.DB
}

// Implement the ReadableDb interface for GormDb
func (db *GormDb) Describe() string {
	return "GORM MySQL Database"
}

func (db *GormDb) Connect() error {
	// GORM handles the connection
	return nil
}

func (db *GormDb) GetCollections() ([]ReadableCollection, error) {
	var collections []ReadableCollection
	// Use GORM's Migrator to get the list of tables
	tables, err := db.Migrator().GetTables()
	if err != nil {
		return nil, err
	}
	for _, table := range tables {
		collections = append(collections, ReadableCollection{Name: table, Type: "table"})
	}
	return collections, nil
}

func (db *GormDb) Close() error {
	// GORM does not require explicit close, but you can implement if needed
	return nil
}

// Implement the ReadWriteDb interface for GormDb
func (db *GormDb) UpsertCollection(name string, collectionType string) error {
	// Implement your upsert logic here
	return nil
}

func (db *GormDb) DeleteCollection(name string) error {
	// Implement your delete logic here
	return nil
}
