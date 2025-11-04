package dbservice

import (
	"fmt"
	"glowstickdb/pkgs/wiredtiger"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
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
		os.RemoveAll("volumes/WT_HOME_TEST")
	}()

	name := "default"

	params := DbParams{
		Name:      name,
		KvService: wtService,
	}

	dbSvc := DatabaseService(params)

	// Create the db
	dbSvc.CreateDB()

	val, key_exists, err := wtService.GetBinaryWithStringKey(CATALOG_TABLE_URI, fmt.Sprintf("db:%s", name))

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
