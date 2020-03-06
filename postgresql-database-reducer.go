package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"time"
)

type ForeignKeys struct {
	ConstraintName    string
	TableName         string
	ColumnName        string
	ForeignTableName  string
	ForeignColumnName string
}

var tableCount = 0

func main() {
	startTime := time.Now()
	connectionString, numberOfRowsToKeep := parseFlags()

	db := connectToDb(connectionString)

	allTableNames := getAllTables(db)
	allForeignKeys := getAllForeignKeys(db)
	transactionDelete(*numberOfRowsToKeep, allTableNames, allForeignKeys, db)
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

func getAllForeignKeys(db *sql.DB) map[string]ForeignKeys {
	allForeignKeyRows, err := db.Query("SELECT tc.constraint_name, tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema WHERE tc.constraint_type = 'FOREIGN KEY'")
	if err != nil {
		fmt.Println(err)
	}

	allForeignKeys := map[string]ForeignKeys{}
	var foreignKeys ForeignKeys

	for allForeignKeyRows.Next() {
		err = allForeignKeyRows.Scan(
			&foreignKeys.ConstraintName,
			&foreignKeys.TableName,
			&foreignKeys.ColumnName,
			&foreignKeys.ForeignTableName,
			&foreignKeys.ForeignColumnName)
		if err != nil {
			fmt.Println(err)
		}
		allForeignKeys[foreignKeys.ForeignTableName+foreignKeys.TableName] = foreignKeys
	}
	fmt.Println("Got all foreign keys")
	return allForeignKeys
}

func CloseTransaction(tx *sql.Tx, commit *bool) {
	if *commit {
		log.Println("Commit sql transaction")
		if err := tx.Commit(); err != nil {
			log.Panic(err)
		}
	} else {
		log.Println("Rollback sql transcation")
		if err := tx.Rollback(); err != nil {
			log.Panic(err)
		}
	}
}

func transactionDelete(numberOfRowsToKeep string, allTableNames []string, allForeignKeys map[string]ForeignKeys, db *sql.DB) {
	//tx, err := db.Begin()
	//if err != nil {
	//	return
	//}
	for _, tableName := range allTableNames {
		for _, foreignTableName := range allTableNames {

			tableForeignKeys := allForeignKeys[tableName + foreignTableName]
			if tableForeignKeys.ConstraintName != "" {

				log.Println(tableForeignKeys.ForeignColumnName)
				log.Println(tableForeignKeys.ForeignTableName)
				log.Println(tableForeignKeys.TableName)
				log.Println(tableForeignKeys.ConstraintName)
				log.Println(tableForeignKeys.ColumnName)
				/*stmt, err := tx.Prepare("SET CONSTRAINTS"+ tableForeignKeys.ForeignColumnName +"DEFERRED") // some raw sql
				if err != nil {
						return
				}
				res, err := stmt.Exec() // some var args
				if err != nil {
					return
				}*/
			}
		}
	}
	//commitTx := false
	//defer CloseTransaction(tx, &commitTx)
	//defer stmt.Close()
	//
	//// Second sql query
	//stmt, err := tx.Prepare("DELETE FROM " + tableName + " USING "+ ForeignTableName +" WHERE ID NOT IN (SELECT id FROM " + tableName + " order by id LIMIT " + numberOfRowsToKeep + ")") // some raw sql
	//if err != nil {
	//	return
	//}
	//defer stmt.Close()
	//
	//res, err := stmt.Exec() // some var args
	//if err != nil {
	//	return
	//}
	//// success, commit and return result
	//commitTx = true
	//return
}

