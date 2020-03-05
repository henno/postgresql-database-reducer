package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"sync"
	"time"
)


var tableCount = 0

func main() {
	startTime := time.Now()
	connectionString, numberOfRowsToKeep := parseFlags()

	db := connectToDb(connectionString)

	allTableNames := getAllTables(db)

	Delete(*numberOfRowsToKeep, allTableNames, db)
	fmt.Println("Overall time: ", time.Since(startTime))
}

func parseFlags() (string, *string) {
	host := flag.String("h", "127.0.0.1", "Host address")
	port := flag.Int("p", 5432, "Database port to connect to")
	user := flag.String("u", "postgres", "Database username")
	password := flag.String("pw", "secret", "Database password")
	dbname := flag.String("d", "database", "Database name")
	numberOfRowsToKeep := flag.String("r", "1000", "Number of rows to keep")
	flag.Parse()
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		*host, *port, *user, *password, *dbname)
	return connectionString, numberOfRowsToKeep
}

func connectToDb(connectionString string) *sql.DB {

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		fmt.Println(err)
	}

	err = db.Ping()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("DB Connected")
	return db
}

func getAllTables(db *sql.DB) []string {
	allTableNameRows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	if err != nil {
		fmt.Println(err)
	}
	var allTableNames []string
	var tableName string

	for allTableNameRows.Next() {

		err = allTableNameRows.Scan(&tableName)
		if err != nil {
			fmt.Println(err)
		}
		allTableNames = append(allTableNames, tableName)

	}
	fmt.Println("Got all tables")
	return allTableNames
}

func Delete(numberOfRowsToKeep string, allTableNames []string, db *sql.DB) {
	var wgDelete sync.WaitGroup

	for _, tableName := range allTableNames {

		wgDelete.Add(1)
		go DeleteRows(tableName, numberOfRowsToKeep, db, &wgDelete)
	}
	wgDelete.Wait()
}

func DeleteRows(tableName string, numberOfRowsToKeep string, db *sql.DB, wgDelete *sync.WaitGroup) {
	defer wgDelete.Done()

	_, disableTriggerErr := db.Exec("ALTER TABLE " + tableName + " DISABLE TRIGGER ALL")
	if disableTriggerErr != nil {
		fmt.Println(disableTriggerErr)
	}

	result, deleteErr := db.Exec("DELETE FROM " + tableName + " WHERE ID NOT IN (SELECT id FROM " + tableName + " order by id LIMIT " + numberOfRowsToKeep + ")")
	if deleteErr != nil {
		fmt.Println(deleteErr)
	} else {
		count, rowsAffectedErr := result.RowsAffected()
		if rowsAffectedErr != nil {
			fmt.Println(rowsAffectedErr)
		} else {
			log.Println("Deleted from: ", tableName ," deleted rows: ", count)
		}
	}


	_, enableTriggerErr := db.Exec("ALTER TABLE " + tableName + " ENABLE TRIGGER ALL")
	if enableTriggerErr != nil {
		fmt.Println(enableTriggerErr)
	}

	tableCount++
	log.Println("Table number: ", tableCount, "Deleted:", tableName)

}
