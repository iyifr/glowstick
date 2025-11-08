package dbservice

import (
	"fmt"
	wt "glowstickdb/pkgs/wiredtiger"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type DbCatalogEntry struct {
	UUID   string            `bson:"_uuid"`
	Name   string            `bson:"name"`
	Config map[string]string `bson:"config"`
}

type CollectionIndex struct {
	Id   string                 `bson:"_id"`
	Key  map[string]int         `bson:"key"` // field name -> sort order/type (e.g., 1 for asc, -1 for desc)
	Name string                 `bson:"name"`
	Ns   string                 `bson:"ns"`             // namespace: "db.collection"
	Type string                 `bson:"type"`           // index type, e.g., "single", "2dsphere", etc.
	V    int                    `bson:"v"`              // version number
	Opts map[string]interface{} `bson:"opts,omitempty"` // additional index options, optional
}

type CollectionCatalogEntry struct {
	Id               primitive.ObjectID `bson:"_id"`
	Ns               string             `bson:"ns"`
	TableUri         string             `bson:"table_uri"`
	IndexTableUriMap map[string]string  `bson:"index_table_uri_map,omitempty"`
	Indexes          []CollectionIndex  `bson:"indexes,omitempty"`
	CreatedAt        primitive.DateTime `bson:"createdAt"`
	UpdatedAt        primitive.DateTime `bson:"updatedAt"`
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
	kv := s.KvService

	// Pass in the kv service to init tables (to avoid one-off failures)
	err := InitTablesHelper(kv)
	if err != nil {
		return err
	}

	if len(collection_name) == 0 {
		return fmt.Errorf("collection name cannot be empty")
	}

	collectionId := primitive.NewObjectID()
	collectionTableUri := fmt.Sprintf("table:collection-%s-%s", collectionId.Hex(), s.Name)

	catalogEntry := CollectionCatalogEntry{
		Id:        collectionId,
		Ns:        fmt.Sprintf("%s.%s", s.Name, collection_name),
		TableUri:  collectionTableUri,
		CreatedAt: primitive.NewDateTimeFromTime(time.Now()),
		UpdatedAt: primitive.NewDateTimeFromTime(time.Now()),
	}

	doc, err := bson.Marshal(catalogEntry)

	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection]: Failed to encode catalog entry")
	}

	err = kv.PutBinaryWithStringKey(CATALOG_TABLE_URI, fmt.Sprintf("%s.%s", s.Name, collection_name), doc)

	if err != nil {
		return fmt.Errorf("failed to write db catalog entry")
	}

	return nil
}

func (s *GDBService) ListCollections() error {
	return nil
}

func InitTablesHelper(wtService wt.WTService) error {
	if _, err := os.Stat("volumes/WT_HOME"); os.IsNotExist(err) {
		if mkErr := os.MkdirAll("volumes/WT_HOME", 0755); mkErr != nil {
			return fmt.Errorf("failed to create volumes/db_files: %w", mkErr)
		}
	}

	if err := wtService.CreateTable(CATALOG_TABLE_URI, "key_format=u,value_format=u"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}
