package dbservice

import (
	"fmt"
	"glowstickdb/pkgs/faiss"
	wt "glowstickdb/pkgs/wiredtiger"
	"net/url"
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
	VectorIndexUri   string             `bson:"vector_index_uri"`
	IndexTableUriMap map[string]string  `bson:"index_table_uri_map,omitempty"`
	Indexes          []CollectionIndex  `bson:"indexes,omitempty"`
	CreatedAt        primitive.DateTime `bson:"createdAt"`
	UpdatedAt        primitive.DateTime `bson:"updatedAt"`
}

type CollectionStats struct {
	Doc_Count         int
	Vector_Index_Size float64
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
		Id: collectionId,
		// Namespace: parent db of the collection.collection name (db.finance_tenant)
		Ns: fmt.Sprintf("%s.%s", s.Name, collection_name),
		// The wiredtiger table where the collection's document
		TableUri:       collectionTableUri,
		VectorIndexUri: fmt.Sprintf("%s_%s", collection_name, ".index"),
		CreatedAt:      primitive.NewDateTimeFromTime(time.Now()),
		UpdatedAt:      primitive.NewDateTimeFromTime(time.Now()),
	}

	err = s.KvService.CreateTable(collectionTableUri, "key_format=u,value_format=u")
	if err != nil {
		fmt.Printf("[GDBSERVICE:CreateCollection:Goroutine] Failed to create table %s: %v\n", collectionTableUri, err)
	}

	doc, err := bson.Marshal(catalogEntry)

	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection]: Failed to encode catalog entry")
	}

	err = kv.PutBinaryWithStringKey(CATALOG_TABLE_URI, fmt.Sprintf("%s.%s", s.Name, collection_name), doc)

	// STATS
	// Create entry in hot stats table
	statsEntry := CollectionStats{
		Doc_Count:         0,
		Vector_Index_Size: 0,
	}

	stats_doc, _ := bson.Marshal(statsEntry)

	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection]: Failed to encode catalog entry")
	}

	err = kv.PutBinaryWithStringKey(STATS_TABLE_URI, fmt.Sprintf("%s.%s", s.Name, collection_name), stats_doc)

	if err != nil {
		return fmt.Errorf("failed to write db catalog entry")
	}

	return nil
}

func (s *GDBService) InsertDocumentsIntoCollection(collection_name string, documents []GlowstickDocument) error {
	kv := s.KvService
	vectr := faiss.FAISS()

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	val, exists, err := kv.GetBinary(CATALOG_TABLE_URI, []byte(collectionDefKey))

	if !exists {
		return fmt.Errorf("collection could not be found in the db")
	}

	if err != nil {
		return err
	}

	var collection CollectionCatalogEntry

	bson.Unmarshal(val, &collection)

	vectorIndexUri := collection.VectorIndexUri

	var filePath string

	u, err := url.Parse(vectorIndexUri)
	if err != nil {
		return fmt.Errorf("failed to parse vector index URI: %v", err)
	}
	filePath = u.Path

	idx, err := vectr.ReadIndex(filePath)

	if err != nil {
		const dim = 1536 // TODO: ensure correct embedding dimension
		const indexDesc = "Flat"
		idx, err = vectr.IndexFactory(dim, indexDesc, faiss.MetricL2)
		if err != nil {
			return fmt.Errorf("failed to create new IVF index for collection: %v (after failing to load old: %w)", err, err)
		}
		// Train index with a slice of the first few document embeddings.
		numTrain := 1 // For IVF12, train with the first 12 documents if available
		if len(documents) < numTrain {
			return fmt.Errorf("cannot train IVF index: number of documents (%d) is less than the number of clusters (%d)", len(documents), numTrain)
		}
		trainData := make([]float32, 0, dim*numTrain)
		for i := 0; i < numTrain; i++ {
			doc := documents[i]
			if len(doc.Embedding) != dim {
				return fmt.Errorf("embedding dim mismatch: got %d, want %d", len(doc.Embedding), dim)
			}
			trainData = append(trainData, doc.Embedding...)
		}
		if err := idx.Train(trainData, numTrain); err != nil {
			return fmt.Errorf("failed to train new IVF index: %v", err)
		}
		if writeErr := idx.WriteToFile(filePath); writeErr != nil {
			return fmt.Errorf("failed to persist new IVF index to %s: %v", filePath, writeErr)
		}
	}

	hot_stats, _, err := kv.GetBinary(STATS_TABLE_URI, []byte(collectionDefKey))

	if err != nil {
		return fmt.Errorf("failed to fetch hot stats:%s", err)
	}

	var hot_stats_doc CollectionStats

	err = bson.Unmarshal(hot_stats, &hot_stats_doc)
	if err != nil {
		return fmt.Errorf("failed to unmarshal hot stats bson into struct:%s", err)
	}

	destTableURI := collection.TableUri

	for index, doc := range documents {
		doc_bytes, err := bson.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document to BSON: %v", err)
		}
		key := doc._Id[:]

		fmt.Printf("Inserting document %d with key bytes: %x\n", index, key)

		if err := s.KvService.PutBinary(destTableURI, key, doc_bytes); err != nil {
			return fmt.Errorf("failed to insert document with _id %s: %v", doc._Id.Hex(), err)
		}

		err = idx.Add(doc.Embedding, 1)
		if err != nil {
			return fmt.Errorf("failed to add embedding to index for _id %s: %v", doc._Id.Hex(), err)
		}
		hot_stats_doc.Doc_Count += 1
	}

	info, err := os.Stat(filePath)

	if err != nil {
		return fmt.Errorf("failed to read file info from vector index file")
	}

	hot_stats_doc.Vector_Index_Size += float64(info.Size())

	bytes, err := bson.Marshal(hot_stats_doc)

	if err != nil {
		return fmt.Errorf("failed to marshal hot stats during write")
	}
	err = kv.PutBinary(STATS_TABLE_URI, []byte(collectionDefKey), bytes)

	if err != nil {
		return fmt.Errorf("failed to write hot stats: %s", err)
	}

	if err := idx.WriteToFile(filePath); err != nil {
		return fmt.Errorf("writeToFile failed: %v", err)
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

	if err := wtService.CreateTable(STATS_TABLE_URI, "key_format=u,value_format=u"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// func float64SliceToFloat32(xs []float64) []float32 {
// 	result := make([]float32, len(xs))
// 	for i, v := range xs {
// 		result[i] = float32(v)
// 	}
// 	return result
// }
