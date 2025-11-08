package dbservice

import (
	wt "glowstickdb/pkgs/wiredtiger"
)

var CATALOG_TABLE_URI = "table:_catalog"

type DBService interface {
	CreateDB() error
	DeleteDB(name string) error
	CreateCollection(collection_name string) error
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
