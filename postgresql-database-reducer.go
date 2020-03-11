package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"strings"
	"sync"
	"time"
)

type ForeignKeys struct {
	ConstraintName    string
	TableName         string
	ColumnName        string
	ForeignTableName  string
	ForeignColumnName string
}

type toDeleteStruct struct {
	foreignTableName  string
	foreignColumnName string
	ColumnName        string
	IDs               map[string]string
}

var deletedIDSCount = 0
var deletedTablesCount = 0

func main() {
	startTime := time.Now()
	connectionString, numberOfRowsToKeep := parseFlags()

	db := connectToDb(connectionString)

	allTableNames := getAllTables(db)
	allForeignKeys := getAllForeignKeys(db)
	DeleteNumberOfRows(*numberOfRowsToKeep, allTableNames, db)
	FindAndDeleteOrphans(allTableNames, allForeignKeys, db)
	fmt.Println("Overall time: ", time.Since(startTime))
}

func parseFlags() (string, *string) {
	host := flag.String("h", "localhost", "Host address")
	port := flag.Int("p", 5432, "Database port to connect to")
	user := flag.String("u", "postgres", "Database username")
	password := flag.String("pw", "", "Database password")
	dbname := flag.String("d", "testdb", "Database name")
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

func DeleteNumberOfRows(numberOfRowsToKeep string, allTableNames []string, db *sql.DB) {
	var wgDelete sync.WaitGroup

	for _, tableName := range allTableNames {

		wgDelete.Add(1)
		go DelRowsFromDB(tableName, numberOfRowsToKeep, db, &wgDelete)
	}
	wgDelete.Wait()
}

// VAJA VEEL TUUNIDA
func DelRowsFromDB(tableName string, numberOfRowsToKeep string, db *sql.DB, wgDelete *sync.WaitGroup) {
	defer wgDelete.Done()

	//_, err := db.Exec("ALTER TABLE " + tableName + " DISABLE TRIGGER ALL")
	//if err != nil {
	//	log.Fatal("err: ", err)
	//}
	//
	//_, err1 := db.Exec("DELETE FROM " + tableName + " WHERE ID NOT IN (SELECT id FROM " + tableName + " order by id LIMIT " + numberOfRowsToKeep + ")")
	//// Warn when table does not have an id field
	//if err1 != nil {
	//	log.Println("err1: ", err1, "tabelname: ", tableName)
	//}
	//_, err2 := db.Exec("ALTER TABLE " + tableName + " ENABLE TRIGGER ALL")
	//if err2 != nil {
	//	log.Fatal("err2: ",err2)
	//}
	//log.Println("DONE:", tableName)

}

func FindAndDeleteOrphans(allTableNames []string, allForeignKeys map[string]ForeignKeys, db *sql.DB) {

	var toDeleteData = make(map[string]map[string]toDeleteStruct)

	var found = 0

	copyOfAllTableNames := []string{}
	for _, value := range allTableNames {
		copyOfAllTableNames = append(copyOfAllTableNames, value)
	}

	for _, tableName := range allTableNames {
		for _, foreignTableName := range copyOfAllTableNames {

			tableForeignKeys := allForeignKeys[foreignTableName+tableName]

			if tableForeignKeys.ConstraintName != "" {

				TableRows, err := db.Query("SELECT " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " FROM " + tableForeignKeys.TableName + " LEFT JOIN " + tableForeignKeys.ForeignTableName + " AS FKTable ON FKTable." + tableForeignKeys.ForeignColumnName + " = " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " WHERE " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " IS NOT NULL AND FKTable." + tableForeignKeys.ForeignColumnName + " IS NULL")

				if err != nil {
					log.Fatal(err)
				}

				var idToBeDeleted string

				for TableRows.Next() {

					err = TableRows.Scan(&idToBeDeleted)

					if err != nil {
						panic(err.Error())
					}

					//log.Println("SELECT " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " FROM " + tableForeignKeys.TableName + " LEFT JOIN " + tableForeignKeys.ForeignTableName + " ON " + tableForeignKeys.ForeignTableName + "." + tableForeignKeys.ForeignColumnName + " = " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " WHERE " + tableForeignKeys.TableName + "." + tableForeignKeys.ColumnName + " IS NOT NULL AND " + tableForeignKeys.ForeignTableName + "." + tableForeignKeys.ForeignColumnName + " IS NULL")
					//fmt.Println("DELETE FROM " + tableForeignKeys.TableName + " WHERE " + tableForeignKeys.ColumnName + " = " + idToBeDeleted)
					found++

					if _, exists := toDeleteData[tableForeignKeys.TableName]; !exists {
						toDeleteData[tableForeignKeys.TableName] = map[string]toDeleteStruct{}
					}

					if _, exists := toDeleteData[tableForeignKeys.TableName][tableForeignKeys.ConstraintName]; !exists {
						toDeleteData[tableForeignKeys.TableName][tableForeignKeys.ConstraintName] = toDeleteStruct{
							foreignTableName:  tableForeignKeys.ForeignTableName,
							foreignColumnName: tableForeignKeys.ForeignColumnName,
							ColumnName:        tableForeignKeys.ColumnName,
							IDs:               map[string]string{},
						}

					}

					toDeleteData[tableForeignKeys.TableName][tableForeignKeys.ConstraintName].IDs[idToBeDeleted] = idToBeDeleted
				}
			}
		}
	}

	PrepareToDeleteOrphans(toDeleteData, db)
	fmt.Println("\nFound Orphans: ", found)
	fmt.Println("Tables count: ", deletedTablesCount)
	fmt.Println("Deleted Ids count: ", deletedIDSCount)

}

//  PrepareToDeleteOrphans function
func PrepareToDeleteOrphans(toDeleteData map[string]map[string]toDeleteStruct, db *sql.DB) {

	for tabelname, valuemap := range toDeleteData {

		var table string
		var column string
		var IDsSlice []string

		table = tabelname

		for _, toDeleteStructValues := range valuemap {

			column = toDeleteStructValues.ColumnName

			for _, ids := range toDeleteStructValues.IDs {
				IDsSlice = append(IDsSlice, ids)
			}
		}

		// Print delete arguments
		fmt.Println("\ntable: ", table)
		fmt.Println("column: ", column)
		fmt.Println("ids: ", strings.Join(IDsSlice, ","))

		deletedIDSCount += len(IDsSlice)

		DeleteOrphans(table, column, strings.Join(IDsSlice, ","), db)
	}

}

func DeleteOrphans(table string, column string, IDs string, db *sql.DB) {

	_, err := db.Exec("ALTER TABLE " + table + " DISABLE TRIGGER ALL")
	if err != nil {
		log.Fatal("err: ", err)
	}

	result, err1 := db.Exec(`DELETE FROM ` + table + ` WHERE ` + column + ` IN (` + IDs + `)`)
	log.Println(`DELETE FROM ` + table + ` WHERE ` + column + ` IN (` + IDs + `)`)
	if err1 != nil {
		fmt.Println("err1: ", err1)
	}

	AffectedRowsCount, err2 := result.RowsAffected()

	if err2 != nil {
		log.Fatal(err2.Error())
	} else {
		fmt.Printf("RowsAffected: " + fmt.Sprintf("%v", AffectedRowsCount))
		fmt.Println()
	}

	_, err3 := db.Exec("ALTER TABLE " + table + " ENABLE TRIGGER ALL")
	if err3 != nil {
		log.Fatal("err2: ", err3)
	}

	deletedTablesCount++
}
