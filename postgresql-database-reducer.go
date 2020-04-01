package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"reflect"
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

var Orphans = 0
var FoundOrphans = 0
var Iterations = 1
var FindOrphans = true
var toDeleteData = make(map[string]map[string]toDeleteStruct)
var AllRowsAffected int64 = 0

var IDsLimit = 1000
var AvailableConnections = 90
var maxConnections = 90

var DefaultHost = "localhost"
var DefaultPort = 5432
var DefaultUser = "postgres"
var DefaultPassword = "asd123"
var DefaultDbName = "testdb"
var AskNewDB = false

func main() {
	startTime := time.Now()
	fmt.Println("\n<-------------------- Start Time: ", startTime.Format("15:04:05 Monday (2006-01-02) -------------------->"))

	ProgramStart()

	fmt.Printf("-> Time: %v\n", time.Since(startTime))
	fmt.Println("\n<-------------------- Program End  -------------------->")
}

func ProgramStart() {

	connectionString := PrintCurrentDb()

	if AskNewDB {

		fmt.Print("\nDo you want to enter new Host? (Type 'yes' Or Press Enter if No): ")

		var err error
		var Ask string

		_, err = fmt.Scanln(&Ask)
		if err != nil {
			if !strings.Contains(err.Error(), "unexpected newline") {
				fmt.Println(err)
			}
		}

		if Ask == "yes" || Ask == "y" {
			Host, Port, User, Password, DbName := GetHostInfo()
			connectionString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Host, Port, User, Password, DbName)
		}
	}

	db := connectToDb(connectionString)

	//DeleteRows(db)
	StartRemovingOrphans(db)

}

func StartRemovingOrphans(db *sql.DB) {
	AllTableNames := getAllTables(db)
	AllFK := getAllForeignKeys(db)
	FindAndDeleteOrphans(AllTableNames, AllFK, db)
}

func DeleteRows(db *sql.DB) {
	TableName, PrimaryKey, NumberRowsToDelete := GetTableInfo()

	DelRowsFromDB(TableName, PrimaryKey, NumberRowsToDelete, db)
}

func PrintCurrentDb() string {

	FlagHost, FlagPort, FlagUser, FlagPassword, FlagDbName := parseFlags()

	if AskNewDB {
		fmt.Println("\n--> Current Database")
		fmt.Println("\n -> Host: ", fmt.Sprintf("%s", *FlagHost))
		fmt.Println(" -> Port: ", fmt.Sprintf("%d", *FlagPort))
		fmt.Println(" -> User: ", fmt.Sprintf("%s", *FlagUser))
		fmt.Println(" -> Password: ", fmt.Sprintf("%s", *FlagPassword))
		fmt.Println(" -> Database Name: ", fmt.Sprintf("%s", *FlagDbName))
	}

	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", *FlagHost, *FlagPort, *FlagUser, *FlagPassword, *FlagDbName)

	return connectionString
}

func GetTableInfo() (string, string, string) {
	var err error
	var TableName string
	var PrimaryKey string
	var NumberRowsToDelete string

	fmt.Print("\n\nTable Name: ")
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
	host := flag.String("h", DefaultHost, "Host address")
	port := flag.Int("p", DefaultPort, "Database port to connect to")
	user := flag.String("u", DefaultUser, "Database username")
	password := flag.String("pw", DefaultPassword, "Database password")
	dbname := flag.String("d", DefaultDbName, "Database name")
	flag.Parse()

	if *host == DefaultHost {
		AskNewDB = true
	}
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

	fmt.Print("\n--> DB Connected")
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
	fmt.Print(" -> Got all tables")
	return allTableNames
}

func getAllForeignKeys(db *sql.DB) map[string]ForeignKeys {
	allForeignKeyRows, err := db.Query("SELECT " + "tc.constraint_name," + "tc.table_name," + "kcu.column_name," + "ccu.table_name" + " AS " + "foreign_table_name," + "ccu.column_name" + " AS " + "foreign_column_name" + " FROM " + "information_schema.table_constraints" + " AS " + "tc" + " JOIN " + "information_schema.key_column_usage" + " AS " + "kcu" + " ON " + "tc.constraint_name" + " = " + "kcu.constraint_name" + " AND " + "tc.table_schema" + " = " + "kcu.table_schema" + " JOIN " + "information_schema.constraint_column_usage" + " AS " + "ccu" + " ON " + "ccu.constraint_name" + " = " + "tc.constraint_name" + " AND " + "ccu.table_schema" + " = " + "tc.table_schema" + " WHERE " + "tc.constraint_type" + " = 'FOREIGN KEY'")
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
	fmt.Print(" -> Got all foreign keys")
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
		RowsAffected, _ := result.RowsAffected()
		log.Println("RowsAffected:", fmt.Sprintf("%v", RowsAffected))
	}

	log.Println("ALTER TABLE " + TableName + " ENABLE TRIGGER ALL;")
	_, err = db.Exec("ALTER TABLE " + TableName + " ENABLE TRIGGER ALL;")
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Println("-> DONE")

}

func MakeCopyOfSlice(slice []string) []string {

	var copyOfSlice []string

	for _, value := range slice {
		copyOfSlice = append(copyOfSlice, value)
	}

	return copyOfSlice
}

func FindAndDeleteOrphans(allTableNames []string, allForeignKeys map[string]ForeignKeys, db *sql.DB) {

	fmt.Print(" -> Searching Orphans")

	copyOfAllTableNames := MakeCopyOfSlice(allTableNames)

	for FindOrphans {
		for _, tableName := range allTableNames {
			for _, foreignTableName := range copyOfAllTableNames {
				tableForeignKeys := allForeignKeys[foreignTableName+tableName]
				if tableForeignKeys.ConstraintName != "" {
					SearchOrphans(tableForeignKeys.TableName, tableForeignKeys.ColumnName, tableForeignKeys.ForeignTableName, tableForeignKeys.ForeignColumnName, tableForeignKeys.ConstraintName, db)
				}
			}
		}
		IterateOrphans(db)
	}

	fmt.Println("\n-> Iterations:", Iterations-1)
	fmt.Println("-> Orphans:", Orphans)
	fmt.Println("-> RowsAffected:", AllRowsAffected)
}

func IterateOrphans(db *sql.DB) {

	var wgWait sync.WaitGroup

	Tables := reflect.ValueOf(toDeleteData).MapKeys()

	fmt.Printf("\n\n-> Iteration (%d) -> Orphans Found %d -> Tables %d -> %v\n", Iterations, FoundOrphans, len(Tables), Tables)

	if FoundOrphans != 0 {
		wgWait.Add(1)
		PrepareToDeleteOrphans(toDeleteData, db, &wgWait)
		wgWait.Wait()
		Orphans += FoundOrphans
		FoundOrphans = 0
		toDeleteData = make(map[string]map[string]toDeleteStruct)
		Iterations++
	} else {
		FindOrphans = false
	}

}

func SearchOrphans(FKTableName string, FKColumnName string, FKForeignTableName string, FKForeignColumnName string, FKConstraintName string, db *sql.DB) {

	TableRows, err := db.Query("SELECT " + FKTableName + "." +
		FKColumnName + " FROM " + FKTableName +
		" LEFT JOIN " + FKForeignTableName + " AS FKTable ON FKTable." +
		FKForeignColumnName + " = " + FKTableName + "." + FKColumnName +
		" WHERE " + FKTableName + "." + FKColumnName +
		" IS NOT NULL AND FKTable." + FKForeignColumnName + " IS NULL")

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

		if _, exists := toDeleteData[FKTableName]; !exists {
			toDeleteData[FKTableName] = map[string]toDeleteStruct{}
		}

		if _, exists := toDeleteData[FKTableName][FKConstraintName]; !exists {
			toDeleteData[FKTableName][FKConstraintName] = toDeleteStruct{
				foreignTableName:  FKForeignTableName,
				foreignColumnName: FKForeignColumnName,
				ColumnName:        FKColumnName,
				IDs:               map[string]string{},
			}

		}

		toDeleteData[FKTableName][FKConstraintName].IDs[idToBeDeleted] = idToBeDeleted
	}
}

func PrepareToDeleteOrphans(toDeleteData map[string]map[string]toDeleteStruct, db *sql.DB, wgWait *sync.WaitGroup) {

	wgWait.Done()

	var wgDeleteOrphans sync.WaitGroup

	for TableName, ValueMap := range toDeleteData {

		var table string
		var column string
		var IDsSlice []string

		table = TableName

		for _, toDeleteStructValues := range ValueMap {

			column = toDeleteStructValues.ColumnName

			for _, IDs := range toDeleteStructValues.IDs {
				IDsSlice = append(IDsSlice, IDs)
			}
		}

		if len(IDsSlice) >= IDsLimit {
			CutSliceAndDelete(IDsSlice, table, column, IDsLimit, db)
		} else {
			wgDeleteOrphans.Add(1)
			go DeleteOrphans(table, column, IDsSlice, db, &wgDeleteOrphans)
		}

	}

	wgDeleteOrphans.Wait()

}

func CutSliceAndDelete(IDsSlice []string, table string, column string, IDsLimit int, db *sql.DB) {

	var NewIDsSlice []string
	var OrderMapSize = 0

	limit := IDsLimit / 2
	size := len(IDsSlice)
	diff := size % limit
	i := (size - diff) - limit
	iterate := true

	var OrderMap = make(map[int][]string)

	for iterate {
		if i == 0 {
			iterate = false
		} else {
			i -= limit
			size -= limit
			NewIDsSlice = IDsSlice[i:size]

			if _, exists := OrderMap[OrderMapSize]; !exists {
				OrderMap[OrderMapSize] = []string{}
			}

			OrderMap[OrderMapSize] = NewIDsSlice
			OrderMapSize++
		}
	}

	fmt.Printf("\nTable: %s, OrderMapSize: %d, size: %d, oneSlice: %d\n", table, OrderMapSize, size, size/OrderMapSize)

	LimitConnectionDelete(table, column, OrderMap, OrderMapSize, db)

}

func LimitConnectionDelete(table string, column string, OrderMap map[int][]string, OrderMapSize int, db *sql.DB) {

	var wgDeleteByOne sync.WaitGroup

	OpenTable(table, db)

	connect := true
	connections := 0
	var indexToDelete []string
	for _, connections := range OrderMap {
		//fmt.Println(connections, "=>", element)
		indexToDelete = connections
	}

	// Order map on tyhi! WIP
	for batchCounter := 0; batchCounter < OrderMapSize; batchCounter++ {
		if batchCounter%maxConnections == 0 {
			for connect {
				if connections == maxConnections {
					connect = false
				} else {

					wgDeleteByOne.Add(1)
					FasterDeleteByTable(table, column, indexToDelete, db, &wgDeleteByOne)
					connections++
				}
			}
			wgDeleteByOne.Wait()
		}
	}

	SlicesLeft := OrderMapSize - connections

	for i := 0; i < SlicesLeft; i++ {

		wgDeleteByOne.Add(1)
		FasterDeleteByTable(table, column, indexToDelete, db, &wgDeleteByOne)

		connections++
	}

	wgDeleteByOne.Wait()

	CloseTable(table, db)

}

func FasterDeleteByTable(RememberTable string, RememberColumn string, NewIDsSlice []string, db *sql.DB, wgDeleteByOne *sync.WaitGroup) {

	wgDeleteByOne.Done()
	DeleteFromTable(RememberTable, RememberColumn, NewIDsSlice, db)
}

func OpenTable(table string, db *sql.DB) {

	_, err := db.Exec("ALTER TABLE " + table + " DISABLE TRIGGER ALL;")
	if err != nil {
		log.Fatal(err)
	}
}

func CloseTable(table string, db *sql.DB) {

	_, err := db.Exec("ALTER TABLE " + table + " ENABLE TRIGGER ALL;")
	if err != nil {
		log.Fatal(err)
	}
}

func DeleteFromTable(table string, column string, IDsSlice []string, db *sql.DB) {

	IDs := strings.Join(IDsSlice, ",")

	result, err := db.Exec(`DELETE FROM ` + table + ` WHERE ` + column + ` IN (` + IDs + `)`)
	if err != nil {
		if strings.Contains(err.Error(), "out of memory") {
			fmt.Printf("| Out of Memory (%s, %d) |", table, len(IDsSlice))
		} else {
			fmt.Printf("| %v |", err)
		}
	} else {
		RowsAffected, _ := result.RowsAffected()
		AllRowsAffected += RowsAffected
		fmt.Printf("| %s (IDs: %d, RowsAffected: %s) |", table, len(IDsSlice), fmt.Sprintf("%v", RowsAffected))

	}
}

func DeleteOrphans(table string, column string, IDsSlice []string, db *sql.DB, wgDeleteOrphans *sync.WaitGroup) {

	wgDeleteOrphans.Done()

	OpenTable(table, db)
	DeleteFromTable(table, column, IDsSlice, db)
	CloseTable(table, db)
}
