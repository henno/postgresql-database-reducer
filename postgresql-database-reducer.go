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
	//os.Exit(0)
	transactionDelete(*numberOfRowsToKeep, allTableNames, allForeignKeys, db)
	fmt.Println("Overall time: ", time.Since(startTime))
}

func parseFlags() (string, *string) {
	host := flag.String("h", "localhost", "Host address")
	port := flag.Int("p", 5432, "Database port to connect to")
	user := flag.String("u", "postgres", "Database username")
	password := flag.String("pw", "password", "Database password")
	dbname := flag.String("d", "dbname", "Database name")
	numberOfRowsToKeep := flag.String("r", "1000", "Number of rows to keep")
	flag.Parse()
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		*host, *port, *user, *password, *dbname)
	return connectionString, numberOfRowsToKeep
}

func connectToDb(connectionString string) *sql.DB {

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
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

	copyOfAllTableNames := []string{}

	for _, value := range allTableNames {
		copyOfAllTableNames = append(copyOfAllTableNames, value)
	}

	for _, tableName := range allTableNames {

		log.Println("TableName: ", tableName)
		for _, foreignTableName := range copyOfAllTableNames {

			tableForeignKeys := allForeignKeys[foreignTableName+tableName]
			//fmt.Println("LOOK HERE: ", tableForeignKeys)
			if tableForeignKeys.ConstraintName != "" {
				fmt.Println("TableName2: ", tableForeignKeys.TableName)

				//fmt.Println("ConstraintName: ", tableForeignKeys.ConstraintName)
				//fmt.Println()
				//log.Println("tableForeignKeys.ForeignColumnName: ",tableForeignKeys.ForeignColumnName)
				//log.Println("tableForeignKeys.ForeignTableName: ", tableForeignKeys.ForeignTableName)
				//log.Println("tableForeignKeys.ConstraintName: ",tableForeignKeys.ConstraintName)
				//log.Println("tableForeignKeys.ColumnName: ",tableForeignKeys.ColumnName)
				//fmt.Println("SELECTING ALL PROBLEMATIC ROWS: \n")

				log.Println("SELECT * FROM " + tableForeignKeys.TableName + " LEFT JOIN " + tableForeignKeys.ForeignTableName + " ON " + tableForeignKeys.ForeignTableName + "." + tableForeignKeys.ForeignColumnName + " = " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " WHERE " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " IS NOT NULL AND " + tableForeignKeys.ForeignTableName + "." + tableForeignKeys.ForeignColumnName + " IS NULL")
				TableRows, err := db.Query("SELECT " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " FROM " + tableForeignKeys.TableName + " LEFT JOIN " + tableForeignKeys.ForeignTableName + " AS FKTable ON FKTable." + tableForeignKeys.ForeignColumnName + " = " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " WHERE " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " IS NOT NULL AND FKTable." + tableForeignKeys.ForeignColumnName + " IS NULL LIMIT 10")
				if err != nil {
					log.Fatal(err)
				}

				var idToBeDeleted string

				for TableRows.Next() {

					err = TableRows.Scan(&idToBeDeleted)

					if err != nil {
						panic(err.Error())
					}

					//tablerowsSLICE = append(tablerowsSLICE, count)

					fmt.Println("DELETE FROM " + tableForeignKeys.TableName + " WHERE " + tableForeignKeys.ColumnName + " = " + idToBeDeleted)

				}

				//fmt.Println(tablerowsSLICE)

				//for _, value := range tablerowsSLICE {
				//	fmt.Println(value)
				//}
				//fmt.Println(tablerowsSLICE)

				//SELECT * FROM Tablename left join foreignTableName on foreignTableName.foreigncolumnName = tablename.columnName WHERE tableName.columname IS NOT NULL AND foreignTableName.foreignColumnName IS NULL

				//SELECT * FROM ? LEFT JOIN ? ON ?.? = ?.? WHERE ?.? IS NOT NULL AND ?.? IS NULL
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
