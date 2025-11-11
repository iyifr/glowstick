package dbservice

import (
	wt "glowstickdb/pkgs/wiredtiger"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

var CATALOG_TABLE_URI = "table:_catalog"
var STATS_TABLE_URI = "table:_stats"

type GlowstickDocument struct {
	_Id       primitive.ObjectID `bson:"_id"`
	Content   string             `bson:"content"`
	Embedding []float32          `bson:"embedding"`
	Metadata  interface{}        `bson:"metadata"` // Any BSON- and JSON-serializable type
}

type DBService interface {
	CreateDB() error
	DeleteDB(name string) error
	CreateCollection(collection_name string) error
	InsertDocumentsIntoCollection(collection_name string, documents []GlowstickDocument) error
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
