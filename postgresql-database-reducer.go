package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strings"
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
var Orphans = 0

func main() {
	startTime := time.Now()
	fmt.Println("\n<-------------------- Start Time: ", startTime.Format("15:04:05 Monday (2006-01-02) -------------------->\n"))

	ProgramStart()

	fmt.Println("\n------> Overall time:", time.Since(startTime), "<------")
}

func ProgramStart() {

	connectionString := PrintCurrentDb()

	fmt.Print("\nDo you want to enter new Host? (Type 'yes' Or Press Enter if No): ")

	var err error
	var Ask string

	_, err = fmt.Scanln(&Ask)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected newline") {
			fmt.Println("\n-----------------------> Enter Table name, Primary key and Number of Rows to Delete <-----------------------")
		}
	} else {
		fmt.Println("Error: ", err)
	}

	if Ask == "yes" || Ask == "y" {
		Host, Port, User, Password, DbName := GetHostInfo()
		connectionString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Host, Port, User, Password, DbName)
	}

	db := connectToDb(connectionString)

	TableName, PrimaryKey, NumberRowsToDelete := GetTableInfo()

	DelRowsFromDB(TableName, PrimaryKey, NumberRowsToDelete, db)

}

func PrintCurrentDb() string {

	FlagHost, FlagPort, FlagUser, FlagPassword, FlagDbName := parseFlags()

	fmt.Println("-----------------------> Current Database Host <-----------------------")
	fmt.Println("\nHost: ", fmt.Sprintf("%s", *FlagHost))
	fmt.Println("Port: ", fmt.Sprintf("%d", *FlagPort))
	fmt.Println("User: ", fmt.Sprintf("%s", *FlagUser))
	fmt.Println("Password: ", fmt.Sprintf("%s", *FlagPassword))
	fmt.Println("Database Name: ", fmt.Sprintf("%s", *FlagDbName))

	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", *FlagHost, *FlagPort, *FlagUser, *FlagPassword, *FlagDbName)

	return connectionString
}

func GetTableInfo() (string, string, string) {
	var err error
	var TableName string
	var PrimaryKey string
	var NumberRowsToDelete string

	fmt.Print("Table Name: ")
	_, err = fmt.Scanln(&TableName)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected newline") {
			fmt.Println("No Table Name")
			os.Exit(1)
		} else {
			fmt.Println("Error: ", err)
		}

	}

	fmt.Print("Primary key: ")
	_, err = fmt.Scanln(&PrimaryKey)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected newline") {
			fmt.Println("No Primary key")
			os.Exit(1)
		} else {
			fmt.Println("Error: ", err)
		}

	}

	fmt.Print("Rows to Delete: ")
	_, err = fmt.Scanln(&NumberRowsToDelete)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected newline") {
			fmt.Println("No Rows")
			os.Exit(1)
		} else {
			fmt.Println("Error: ", err)
		}

	}

	return TableName, PrimaryKey, NumberRowsToDelete
}

func GetHostInfo() (string, int, string, string, string) {

	var err error
	var Host string
	var Port int
	var User string
	var Password string
	var DbName string

	fmt.Print("Host: ")
	_, err = fmt.Scanln(&Host)
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("&", Host)
	}

	fmt.Print("Port: ")
	_, err = fmt.Scanln(&Port)
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("&", Port)
	}

	fmt.Print("User: ")
	_, err = fmt.Scanln(&User)
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("&", User)
	}

	fmt.Print("Password: ")
	_, err = fmt.Scanln(&Password)
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("&", Password)
	}

	fmt.Print("Database Name: ")
	_, err = fmt.Scanln(&DbName)
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("&", DbName)
	}

	return Host, Port, User, Password, DbName
}

func parseFlags() (*string, *int, *string, *string, *string) {
	host := flag.String("h", "localhost", "Host address")
	port := flag.Int("p", 5432, "Database port to connect to")
	user := flag.String("u", "postgres", "Database username")
	password := flag.String("pw", "asd123", "Database password")
	dbname := flag.String("d", "testdb", "Database name")
	//numberOfRowsToKeep := flag.String("r", "1000", "Number of rows to keep")
	flag.Parse()
	//connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
	//	*host, *port, *user, *password, *dbname)
	return host, port, user, password, dbname
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

	fmt.Println("--> DB Connected")
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
	fmt.Println("--> Got all tables")
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
	fmt.Println("--> Got all foreign keys")
	return allForeignKeys
}

func DelRowsFromDB(TableName string, PrimaryKey string, NumberRowsToDelete string, db *sql.DB) {

	var err error
	var result sql.Result

	fmt.Println()

	log.Println("ALTER TABLE " + TableName + " DISABLE TRIGGER ALL;")
	_, err = db.Exec("ALTER TABLE " + TableName + " DISABLE TRIGGER ALL;")
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Println("DELETE FROM " + TableName + " WHERE " + PrimaryKey + " = any (array(SELECT " + PrimaryKey + " FROM " + TableName + " ORDER BY " + PrimaryKey + " LIMIT " + NumberRowsToDelete + "));")
	result, err = db.Exec("DELETE FROM " + TableName + " WHERE " + PrimaryKey + " = any (array(SELECT " + PrimaryKey + " FROM " + TableName + " ORDER BY " + PrimaryKey + " LIMIT " + NumberRowsToDelete + "));")

	if err != nil {
		log.Fatal(err.Error())
	} else {
		RowsAffected1, err := result.RowsAffected()

		if err != nil {
			log.Fatal(err.Error())
		} else {
			log.Println("RowsAffected:", fmt.Sprintf("%v", RowsAffected1))
		}
	}

	log.Println("ALTER TABLE " + TableName + " ENABLE TRIGGER ALL;")
	_, err = db.Exec("ALTER TABLE " + TableName + " ENABLE TRIGGER ALL;")
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Println("-----> DONE")

}

func FindAndDeleteOrphans(allTableNames []string, allForeignKeys map[string]ForeignKeys, db *sql.DB) {

	var toDeleteData = make(map[string]map[string]toDeleteStruct)

	copyOfAllTableNames := []string{}
	var FindOrphans = true
	var FoundOrphans = 0

	for _, value := range allTableNames {
		copyOfAllTableNames = append(copyOfAllTableNames, value)
	}

	Iterations := 1

	for FindOrphans {
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

						FoundOrphans++

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

		fmt.Println("\n------> Iteration:", Iterations)
		fmt.Println("------> Orphans Found:", FoundOrphans)
		if FoundOrphans != 0 {
			PrepareToDeleteOrphans(toDeleteData, db)
			Orphans += FoundOrphans
			FoundOrphans = 0
			Iterations++
		} else {
			FindOrphans = false
			fmt.Println("------> All Orphans Deleted <------")
		}

	}

	fmt.Println("\n-> Iterations Count:", Iterations)

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

		// Print delete arguments (Tables with Orphans)

		//fmt.Println("\ntable: ", table)
		//fmt.Println("column: ", column)
		//fmt.Println("ids: ", strings.Join(IDsSlice, ","))

		deletedIDSCount += len(IDsSlice)

		DeleteOrphans(table, column, strings.Join(IDsSlice, ","), db)
	}

}

func DeleteOrphans(table string, column string, IDs string, db *sql.DB) {

	_, err := db.Exec("ALTER TABLE " + table + " DISABLE TRIGGER ALL;")
	if err != nil {
		log.Fatal("err: ", err)
	}

	result2, err1 := db.Exec(`DELETE FROM ` + table + ` WHERE ` + column + ` IN (` + IDs + `);`)
	log.Println(`DELETE FROM ` + table + ` WHERE ` + column + ` IN (` + IDs + `)`)
	if err1 != nil {
		fmt.Println("err1: ", err1)
	}

	RowsAffected2, err2 := result2.RowsAffected()

	if err2 != nil {
		log.Fatal(err2.Error())
	} else {
		fmt.Println("RowsAffected:", fmt.Sprintf("%v", RowsAffected2))

	}

	_, err3 := db.Exec("ALTER TABLE " + table + " ENABLE TRIGGER ALL;")
	if err3 != nil {
		log.Fatal("err2: ", err3)
	}

}
