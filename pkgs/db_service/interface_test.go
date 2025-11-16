package dbservice

import (
	"fmt"
	"glowstickdb/pkgs/faiss"
	"glowstickdb/pkgs/wiredtiger"
	"math/rand/v2"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var WIREDTIGER_DIR = "volumes/WT_HOME_TEST"

func TestCreateDb(t *testing.T) {
	wtService := wiredtiger.WiredTiger()

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			t.Fatalf("failed to create WT_HOME_TEST dir: %v", mkErr)
		}
	}

	if err := wtService.Open(WIREDTIGER_DIR, "create"); err != nil {
		t.Log("Err occured")
	}

	defer func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
		//os.RemoveAll("volumes/WT_HOME_TEST")
	}()

	name := "default"

	params := DbParams{
		Name:      name,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	dbSvc.CreateDB()

	val, key_exists, err := wtService.GetBinaryWithStringKey(CATALOG, fmt.Sprintf("db:%s", name))

	if !key_exists {
		t.Errorf("DB value not persisted.")
	}

	if err != nil {
		t.Errorf("Error occurred in test: %v", err)
	}

	// Check if value is valid BSON and unmarshal back into struct
	if len(val) == 0 {
		t.Errorf("Returned value was empty ([]byte length == 0)")
	}
	type dbCatalog struct {
		UUID   string            `bson:"_uuid"`
		Name   string            `bson:"name"`
		Config map[string]string `bson:"config"`
	}
	var entry dbCatalog
	if err := bson.Unmarshal(val[:], &entry); err != nil {
		t.Errorf("Returned value was not valid BSON for DbCatalogEntry: %v", err)
	}

	if entry.Name != name {
		t.Errorf("Corrupted or incorrect DB name. Got: %s, want: %s", entry.Name, name)
	}

	if entry.UUID == "" {
		t.Errorf("UUID is missing in returned struct")
	}

	if entry.Config == nil || entry.Config["Index"] != "HNSW" {
		t.Errorf("Config field corrupted or missing expected value: %v", len(entry.Config))
	}

}

func TestCreateCollection(t *testing.T) {
	wtService := wiredtiger.WiredTiger()

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			t.Fatalf("failed to create WT_HOME_TEST dir: %v", mkErr)
		}
	}

	if err := wtService.Open(WIREDTIGER_DIR, "create"); err != nil {
		t.Log("Err occured")
	}

	defer func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
		// os.RemoveAll("volumes/WT_HOME_TEST")
	}()

	dbName := "default"
	collName := "tenant_id_1"

	params := DbParams{
		Name:      dbName,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	err := dbSvc.CreateDB()

	if err != nil {
		t.Errorf("Failed to create Db; %s", err)
	}

	dbSvc.CreateCollection(collName)

	fmt.Printf("URI: %s\n", fmt.Sprintf("%s.%s", dbName, collName))

	val, key_exists, err := wtService.GetBinaryWithStringKey(CATALOG, fmt.Sprintf("%s.%s", dbName, collName))

	if !key_exists {
		t.Errorf("DB value not persisted.")
	}

	if err != nil {
		t.Errorf("Error occurred in test: %v", err)
	}

	// Check if value is valid BSON and unmarshal back into struct
	if len(val) == 0 {
		t.Errorf("Returned value was empty ([]byte length == 0)")
	}

	var entry CollectionCatalogEntry
	unmarshalErr := bson.Unmarshal(val[:], &entry)
	if unmarshalErr != nil {
		t.Errorf("Failed to unmarshal BSON value: %v", unmarshalErr)
	}
	if entry.Ns != fmt.Sprintf("%s.%s", dbName, collName) {
		t.Errorf("Unmarshaled CollectionCatalogEntry Ns does not match: got %s, want %s", entry.Ns, fmt.Sprintf("%s.%s", dbName, collName))
	}

}

func TestInsertDocuments(t *testing.T) {
	wtService := wiredtiger.WiredTiger()

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			t.Fatalf("failed to create WT_HOME_TEST dir: %v", mkErr)
		}
	}

	if err := wtService.Open(WIREDTIGER_DIR, "create"); err != nil {
		t.Log("Err occured")
	}

	defer func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
		// os.RemoveAll("volumes/WT_HOME_TEST")
	}()

	dbName := "default"
	collName := "tenant_id_1"

	params := DbParams{
		Name:      dbName,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	err := dbSvc.CreateDB()
	if err != nil {
		t.Errorf("Failed to create Db; %s", err)
	}

	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Errorf("Failed to create collection: %s", err)
	}

	documents := []GlowstickDocument{
		{
			_Id:       primitive.NewObjectID(),
			Content:   "First example document",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]interface{}{"type": "example", "index": 1},
		},
		{
			_Id:       primitive.NewObjectID(),
			Content:   "Second example document",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]interface{}{"type": "example", "index": 2},
		},
		{
			_Id:       primitive.NewObjectID(),
			Content:   "Third example document",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]interface{}{"type": "example", "index": 3},
		},
	}

	err = dbSvc.InsertDocumentsIntoCollection(collName, documents)
	if err != nil {
		t.Errorf("InsertDocumentsIntoCollection returned error: %v", err)
	}

	collectionDefKey := fmt.Sprintf("%s.%s", dbName, collName)
	val, exists, err := wtService.GetBinary(CATALOG, []byte(collectionDefKey))
	if err != nil {
		t.Fatalf("failed to get collection catalog entry from _catalog: %v", err)
	}
	if !exists {
		t.Fatalf("catalog entry does not exist for collection '%s'", collectionDefKey)
	}

	var catalogEntry CollectionCatalogEntry
	unmarshalErr := bson.Unmarshal(val, &catalogEntry)
	if unmarshalErr != nil {
		t.Fatalf("Failed to unmarshal catalog entry: %v", unmarshalErr)
	}
	collTableURI := catalogEntry.TableUri
	if collTableURI == "" {
		t.Fatalf("Table URI not set in catalog entry for %s", collName)
	}

	for index, doc := range documents {
		docKey := doc._Id[:]
		fmt.Printf("Index: %d, docKey: %x\n", index, docKey)

		record, found, err := wtService.GetBinary(collTableURI, docKey)
		if err != nil {
			t.Errorf("failed to read doc _id=%s from table %s: %v", doc._Id.Hex(), collTableURI, err)
		}
		if !found {
			t.Errorf("inserted doc _id=%s not found in collection physical table %s", doc._Id.Hex(), collTableURI)
		}

		var restoredDoc GlowstickDocument
		if err := bson.Unmarshal(record, &restoredDoc); err != nil {
			t.Errorf("unmarshal failed for _id=%s: %v", doc._Id.Hex(), err)
		}
		if doc.Content != restoredDoc.Content {
			t.Errorf("Retrieved content does not match document saved. Retrieved:%s, Document:%s", restoredDoc.Content, doc.Content)
		}
	}

	statsVal, statsExists, statsErr := wtService.GetBinary(STATS, []byte(collectionDefKey))
	if statsErr != nil {
		t.Errorf("Failed to retrieve _stats entry for collection %s: %v", collName, statsErr)
	}
	if !statsExists {
		t.Errorf("_stats entry missing for collection %s", collName)
	}
	var hotStats CollectionStats
	if statsErr == nil && statsExists {
		if err := bson.Unmarshal(statsVal, &hotStats); err != nil {
			t.Errorf("Unmarshal failed for hot stats: %v", err)
		}

		if int(hotStats.Doc_Count) != len(documents) {
			t.Logf("hot stats: %f", hotStats.Vector_Index_Size)
			t.Errorf("Stats Doc_Count mismatch, got %d, want %d", hotStats.Doc_Count, len(documents))
		}
	}

}

func TestBasicVectorQuery(t *testing.T) {
	wtService := wiredtiger.WiredTiger()

	if _, err := os.Stat(WIREDTIGER_DIR); os.IsNotExist(err) {
		if mkErr := os.MkdirAll(WIREDTIGER_DIR, 0755); mkErr != nil {
			t.Fatalf("failed to create WT_HOME_TEST dir: %v", mkErr)
		}
	}

	if err := wtService.Open(WIREDTIGER_DIR, "create"); err != nil {
		t.Log("Err occured")
	}

	defer func() {
		if err := wtService.Close(); err != nil {
			fmt.Printf("Warning: failed to close connection: %v\n", err)
		}
		// os.RemoveAll("volumes/WT_HOME_TEST")
	}()

	dbName := "default"
	collName := "tenant_id_1"

	params := DbParams{
		Name:      dbName,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	err := dbSvc.CreateDB()
	if err != nil {
		t.Errorf("Failed to create Db; %s", err)
	}

	err = dbSvc.CreateCollection(collName)
	if err != nil {
		t.Errorf("Failed to create collection: %s", err)
	}

	documents := []GlowstickDocument{
		{
			_Id:       primitive.NewObjectID(),
			Content:   "First example document",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]interface{}{"type": "example", "index": 1},
		},
		{
			_Id:       primitive.NewObjectID(),
			Content:   "Second example document",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]interface{}{"type": "example", "index": 2},
		},
		{
			_Id:       primitive.NewObjectID(),
			Content:   "Third example document",
			Embedding: genEmbeddings(1536),
			Metadata:  map[string]interface{}{"type": "example", "index": 3},
		},
	}

	err = dbSvc.InsertDocumentsIntoCollection(collName, documents)
	if err != nil {
		t.Errorf("InsertDocumentsIntoCollection returned error: %v", err)
	}
}

func genEmbeddings(dim int) []float32 {
	fs := faiss.FAISS()
	randVec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		randVec[i] = rand.Float32()
	}
	return fs.NormalizeBatch(randVec, dim)
}
