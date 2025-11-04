package dbservice

import (
	"fmt"
	"os"

	wt "glowstickdb/pkgs/wiredtiger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var CATALOG_TABLE_URI = "table:_catalog"

type DbCatalogEntry struct {
	UUID   string            `bson:"_uuid"`
	Name   string            `bson:"name"`
	Config map[string]string `bson:"config"`
}

type DBService interface {
	CreateDB() error
	DeleteDB(name string) error
	CreateCollection(db string) error
	ListCollections() error
}

type DbParams struct {
	Name        string
	PutIfAbsent bool
	KvService   wt.WTService
}

func DatabaseService(params DbParams) DBService {
	return &GDBService{Name: params.Name, KvService: params.KvService}
}

type GDBService struct {
	Name      string
	KvService wt.WTService
}

func (s *GDBService) CreateDB() error {

	err := InitTablesHelper(s.KvService)

	if err != nil {
		return err
	}

	if s.Name == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	catalogEntry := DbCatalogEntry{
		UUID:   primitive.NewObjectID().Hex(),
		Name:   s.Name,
		Config: map[string]string{"Index": "HNSW"},
	}

	doc, err := bson.Marshal(catalogEntry)

	if err != nil {
		return err
	}

	err = s.KvService.PutBinaryWithStringKey(CATALOG_TABLE_URI, fmt.Sprintf("db:%s", s.Name), doc)

	if err != nil {
		return fmt.Errorf("failed to write db catalog entry")
	}

	return nil
}

func (s *GDBService) DeleteDB(name string) error {
	return nil
}

func (s *GDBService) CreateCollection(collection_name string) error {
	return nil
}

func (s *GDBService) ListCollections() error {
	return nil
}

// Initialize necessary tables
func InitTablesHelper(wtService wt.WTService) error {

	if _, err := os.Stat("volumes/WT_HOME"); os.IsNotExist(err) {
		if mkErr := os.MkdirAll("volumes/WT_HOME", 0755); mkErr != nil {
			return fmt.Errorf("failed to create volumes/db_files: %w", mkErr)
		}
	}

	// Create table
	if err := wtService.CreateTable(CATALOG_TABLE_URI, "key_format=u,value_format=u"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}
