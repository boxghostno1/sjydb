package main

import (
	"github.com/tecbot/gorocksdb"
	"log"
)

func main() {
	log_path := "/gorocksdblog"
	db, err := OpenDB(log_path)
	if err != nil {
		log.Println("fail to open db,", nil, db)
	}
	readOptions := gorocksdb.NewDefaultReadOptions()
	readOptions.SetFillCache(true)
	writeOptions := gorocksdb.NewDefaultWriteOptions()
	writeOptions.SetSync(true)
}