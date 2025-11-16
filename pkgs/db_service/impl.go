package dbservice

import (
	"fmt"
	"glowstickdb/pkgs/faiss"
	wt "glowstickdb/pkgs/wiredtiger"
	"net/url"
	"os"
	"sort"
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

	err = s.KvService.PutBinaryWithStringKey(CATALOG, fmt.Sprintf("db:%s", s.Name), doc)

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
		VectorIndexUri: fmt.Sprintf("%s%s", collection_name, ".index"),
		CreatedAt:      primitive.NewDateTimeFromTime(time.Now()),
		UpdatedAt:      primitive.NewDateTimeFromTime(time.Now()),
	}

	err = s.KvService.CreateTable(collectionTableUri, "key_format=u,value_format=u")
	if err != nil {
		fmt.Printf("[GDBSERVICE:CreateCollection:Goroutine] Failed to create table %s: %v\n", collectionTableUri, err)
		return fmt.Errorf("[GDBSERVICE:CreateCollection:Goroutine] Failed to create table %s: %v", collectionTableUri, err)
	}

	doc, err := bson.Marshal(catalogEntry)

	if err != nil {
		return fmt.Errorf("[GDBSERVICE:CreateCollection]: Failed to encode catalog entry")
	}

	err = kv.PutBinaryWithStringKey(CATALOG, fmt.Sprintf("%s.%s", s.Name, collection_name), doc)

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

	err = kv.PutBinaryWithStringKey(STATS, fmt.Sprintf("%s.%s", s.Name, collection_name), stats_doc)

	if err != nil {
		return fmt.Errorf("failed to write db catalog entry")
	}

	return nil
}

func (s *GDBService) InsertDocumentsIntoCollection(collection_name string, documents []GlowstickDocument) error {
	kv := s.KvService
	vectr := faiss.FAISS()

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)
	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if !exists {
		return fmt.Errorf("collection:%s could not be found in the db", collection_name)
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
		const indexDesc = "Flat"
		idx, err = vectr.IndexFactory(len(documents[0].Embedding), indexDesc, faiss.MetricL2)
		if err != nil {
			return fmt.Errorf("failed to create new vector index for collection: %v (after failing to load old: %w)", err, err)
		}

		if writeErr := idx.WriteToFile(filePath); writeErr != nil {
			return fmt.Errorf("failed to persist new IVF index to %s: %v", filePath, writeErr)
		}
		//return fmt.Errorf("unable to read vector index index from file path:%s", filePath)
	}

	hot_stats, _, err := kv.GetBinary(STATS, []byte(collectionDefKey))

	if err != nil {
		return fmt.Errorf("failed to fetch hot stats:%s", err)
	}

	var hot_stats_doc CollectionStats

	err = bson.Unmarshal(hot_stats, &hot_stats_doc)
	if err != nil {
		return fmt.Errorf("failed to unmarshal hot stats bson into struct:%s", err)
	}

	destTableURI := collection.TableUri

	for _, doc := range documents {
		doc_bytes, err := bson.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document to BSON: %v", err)
		}
		key := doc._Id[:]

		if err := s.KvService.PutBinary(destTableURI, key, doc_bytes); err != nil {
			return fmt.Errorf("failed to insert document with _id %s: %v", doc._Id.Hex(), err)
		}

		err = idx.Add(doc.Embedding, 1)
		var label int64 = -1
		if err != nil {
			return fmt.Errorf("failed to add embedding to index for _id %s: %v", doc._Id.Hex(), err)
		}

		if nTotal, nErr := idx.NTotal(); nErr == nil {
			label = nTotal - 1
		}

		docIDHex := fmt.Sprintf("%x", key)
		err = s.KvService.PutString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, fmt.Sprintf("%d", label), docIDHex)

		if err != nil {
			return fmt.Errorf("failed to write label->docID mapping to table: %v", err)
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
	err = kv.PutBinary(STATS, []byte(collectionDefKey), bytes)

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

func (s *GDBService) QueryCollection(collection_name string, query QueryStruct) ([]GlowstickDocument, error) {
	kv := s.KvService
	vectr_svc := faiss.FAISS()

	docs := []GlowstickDocument{}

	collectionDefKey := fmt.Sprintf("%s.%s", s.Name, collection_name)

	val, exists, err := kv.GetBinary(CATALOG, []byte(collectionDefKey))

	if !exists {
		return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - collection could not be found in the db")
	}

	if err != nil {
		return nil, err
	}

	var collection CollectionCatalogEntry

	bson.Unmarshal(val, &collection)

	vectorIndexUri := collection.VectorIndexUri

	var filePath string

	u, err := url.Parse(vectorIndexUri)
	if err != nil {
		return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - failed to parse vector index URI: %v", err)
	}
	filePath = u.Path

	idx, err := vectr_svc.ReadIndex(filePath)

	if err != nil {
		return nil, fmt.Errorf("could not vector index after specfied file path")
	}

	distances, ids, err := idx.Search(query.QueryEmbedding, 1, int(query.TopK))

	if err != nil {
		return nil, fmt.Errorf("[DB_SERVICE:QueryCollection] - failed to search vector index for query embedding")
	}

	indices := make([]int, len(distances))
	for i := range indices {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		return distances[indices[i]] < distances[indices[j]]
	})

	var lastErr error = err
	for _, index := range indices {
		id := ids[index]
		distance := distances[index]

		// id could be -1 if FAISS returned a "no result"; handle this
		if id < 0 {
			continue
		}

		key := fmt.Sprintf("%d", id)
		val, _, err := kv.GetString(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, key)
		if err != nil {
			fmt.Printf("Failed to get docID for label %s: %v\n", key, err)
			lastErr = err
			continue
		}

		if len(val) != 24 {
			fmt.Printf("Invalid ObjectID hex length: expected 24, got %d for '%s'\n", len(val), val)
			lastErr = fmt.Errorf("invalid ObjectID hex length: expected 24, got %d for '%s'", len(val), val)
			continue
		}

		objectID, err := primitive.ObjectIDFromHex(val)
		if err != nil {
			fmt.Printf("Failed to parse docID '%s' as ObjectID hex: %v\n", val, err)
			lastErr = err
			continue
		}

		// Validate the ObjectID is not empty/zero
		if objectID.IsZero() {
			fmt.Printf("ObjectID is zero/empty for hex '%s'\n", val)
			lastErr = fmt.Errorf("ObjectID is zero/empty for hex '%s'", val)
			continue
		}

		docIDBytes := objectID[:] // Convert ObjectID to raw [12]byte slice
		if len(docIDBytes) != 12 {
			fmt.Printf("Invalid docIDBytes length: expected 12, got %d\n", len(docIDBytes))
			lastErr = fmt.Errorf("invalid docIDBytes length: expected 12, got %d", len(docIDBytes))
			continue
		}

		docBin, _, err := kv.GetBinary(collection.TableUri, docIDBytes)
		if err != nil {
			fmt.Printf("Failed to get document for docID %s in table %s: %v\n", val, collection.TableUri, err)
			lastErr = err
			continue
		}
		if len(docBin) > 0 {
			var doc GlowstickDocument

			if err := bson.Unmarshal(docBin, &doc); err != nil {
				fmt.Printf("Failed to unmarshal BSON for docID %s: %v\n", val, err)
				lastErr = err
				continue
			}

			fmt.Printf("DocID: %s, Distance: %f\n", val, distance)

			if query.MinDistance == 0 || distance < query.MinDistance {
				docs = append(docs, doc)
			} else {
				fmt.Printf("DocID: %s, skipped\n", val)
			}
		}
	}

	return docs, lastErr
}

func InitTablesHelper(wtService wt.WTService) error {
	if _, err := os.Stat("volumes/WT_HOME"); os.IsNotExist(err) {
		if mkErr := os.MkdirAll("volumes/WT_HOME", 0755); mkErr != nil {
			return fmt.Errorf("failed to create volumes/db_files: %w", mkErr)
		}
	}

	if err := wtService.CreateTable(CATALOG, "key_format=u,value_format=u"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := wtService.CreateTable(STATS, "key_format=u,value_format=u"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := wtService.CreateTable(LABELS_TO_DOC_ID_MAPPING_TABLE_URI, "key_format=S,value_format=S"); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
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
