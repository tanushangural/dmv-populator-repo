package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

// Database configuration - same as temp_query_runner.go
const (
	DB_SERVER   = "db-otel-test.cszoyw6q4wm8.us-east-1.rds.amazonaws.com"
	DB_PORT     = "1433"
	DB_USER     = "aryan_db"
	DB_PASSWORD = "BabaElaichi"
	DB_NAME     = "AdventureWorks2019" // Use AdventureWorks2019 for DMV population
)

// Configuration for DMV population
const (
	TARGET_QUERY_COUNT       = 10000 // Target 350k queries (3.5 lakhs)
	CONCURRENT_WORKERS       = 5     // 12 concurrent workers
	RUN_DURATION_MINUTES     = 10     // Run for 25 minutes
	PROGRESS_REPORT_INTERVAL = 200   // Report progress every 5k queries
)

// SQL Server configuration to prevent query anonymization
const PreventAnonymizationSQL = `
-- Disable query plan anonymization and optimize for execution plan tracking
SET NOCOUNT ON;

-- Enable query store if not already enabled (helps with plan tracking)
IF NOT EXISTS (SELECT 1 FROM sys.database_query_store_options WHERE current_state = 1)
BEGIN
    ALTER DATABASE AdventureWorks2019 SET QUERY_STORE = ON;
    ALTER DATABASE AdventureWorks2019 SET QUERY_STORE (OPERATION_MODE = READ_WRITE);
END

-- Set trace flags to improve plan caching and prevent anonymization
DBCC TRACEON(2528); -- Disable parallelism for better plan diversity
DBCC TRACEON(8048); -- Force NUMA partitioning for plan cache
DBCC TRACEON(4199); -- Enable additional query optimizations

PRINT 'SQL Server configured to prevent query anonymization and optimize plan tracking';
`

// AdventureWorks2019 table metadata for dynamic query generation
var AdventureWorksTables = []TableInfo{
	// Sales tables
	{Schema: "Sales", Name: "SalesOrderHeader", Columns: []string{"SalesOrderID", "CustomerID", "SalesPersonID", "OrderDate", "DueDate", "ShipDate", "Status", "TotalDue", "TaxAmt", "Freight"}},
	{Schema: "Sales", Name: "SalesOrderDetail", Columns: []string{"SalesOrderID", "SalesOrderDetailID", "ProductID", "OrderQty", "UnitPrice", "LineTotal", "SpecialOfferID"}},
	{Schema: "Sales", Name: "Customer", Columns: []string{"CustomerID", "PersonID", "StoreID", "TerritoryID", "AccountNumber", "ModifiedDate"}},
	{Schema: "Sales", Name: "SalesPerson", Columns: []string{"BusinessEntityID", "TerritoryID", "SalesQuota", "Bonus", "CommissionPct", "SalesYTD", "SalesLastYear"}},
	{Schema: "Sales", Name: "Store", Columns: []string{"BusinessEntityID", "Name", "SalesPersonID", "Demographics", "ModifiedDate"}},

	// Production tables
	{Schema: "Production", Name: "Product", Columns: []string{"ProductID", "Name", "ProductNumber", "Color", "StandardCost", "ListPrice", "Size", "Weight", "ProductCategoryID", "ProductSubcategoryID"}},
	{Schema: "Production", Name: "ProductCategory", Columns: []string{"ProductCategoryID", "Name", "ModifiedDate"}},
	{Schema: "Production", Name: "ProductSubcategory", Columns: []string{"ProductSubcategoryID", "ProductCategoryID", "Name", "ModifiedDate"}},
	{Schema: "Production", Name: "WorkOrder", Columns: []string{"WorkOrderID", "ProductID", "OrderQty", "StockedQty", "ScrappedQty", "StartDate", "EndDate", "DueDate"}},

	// Person tables
	{Schema: "Person", Name: "Person", Columns: []string{"BusinessEntityID", "PersonType", "FirstName", "MiddleName", "LastName", "EmailPromotion", "ModifiedDate"}},
	{Schema: "Person", Name: "Address", Columns: []string{"AddressID", "AddressLine1", "AddressLine2", "City", "StateProvinceID", "PostalCode", "ModifiedDate"}},
	{Schema: "Person", Name: "StateProvince", Columns: []string{"StateProvinceID", "StateProvinceCode", "CountryRegionCode", "Name", "TerritoryID", "ModifiedDate"}},

	// HumanResources tables
	{Schema: "HumanResources", Name: "Employee", Columns: []string{"BusinessEntityID", "NationalIDNumber", "LoginID", "JobTitle", "BirthDate", "MaritalStatus", "Gender", "HireDate", "VacationHours", "SickLeaveHours"}},
	{Schema: "HumanResources", Name: "Department", Columns: []string{"DepartmentID", "Name", "GroupName", "ModifiedDate"}},

	// Purchasing tables
	{Schema: "Purchasing", Name: "PurchaseOrderHeader", Columns: []string{"PurchaseOrderID", "RevisionNumber", "Status", "EmployeeID", "VendorID", "ShipMethodID", "OrderDate", "TotalDue", "TaxAmt", "Freight"}},
	{Schema: "Purchasing", Name: "PurchaseOrderDetail", Columns: []string{"PurchaseOrderID", "PurchaseOrderDetailID", "DueDate", "OrderQty", "ProductID", "UnitPrice", "ReceivedQty", "RejectedQty"}},
	{Schema: "Purchasing", Name: "Vendor", Columns: []string{"BusinessEntityID", "AccountNumber", "Name", "CreditRating", "PreferredVendorStatus", "ActiveFlag"}},
}

type TableInfo struct {
	Schema  string
	Name    string
	Columns []string
}

type QueryStats struct {
	TotalQueries       int64
	SuccessfulQueries  int64
	FailedQueries      int64
	TotalExecutionTime time.Duration
	QueriesPerSecond   float64
	StartTime          time.Time
	EndTime            time.Time
}

type WorkerStats struct {
	WorkerID          int
	QueriesExecuted   int64
	QueriesSuccessful int64
	QueriesFailed     int64
	ExecutionTime     time.Duration
	LastError         string
}

// Query templates for different types of operations
var QueryTemplates = []string{
	// Simple SELECT queries
	"SELECT TOP %d %s FROM %s.%s WHERE %s IS NOT NULL",
	"SELECT %s, COUNT(*) as cnt FROM %s.%s GROUP BY %s HAVING COUNT(*) > 1",
	"SELECT DISTINCT %s FROM %s.%s WHERE %s IS NOT NULL ORDER BY %s",

	// JOIN queries
	"SELECT TOP %d a.%s, b.%s FROM %s.%s a INNER JOIN %s.%s b ON a.%s = b.%s WHERE a.%s IS NOT NULL",
	"SELECT a.%s, b.%s, COUNT(*) FROM %s.%s a LEFT JOIN %s.%s b ON a.%s = b.%s GROUP BY a.%s, b.%s",

	// Aggregate queries
	"SELECT %s, COUNT(*) as total FROM %s.%s GROUP BY %s",
	"SELECT %s, COUNT(*) FROM %s.%s WHERE %s IS NOT NULL GROUP BY %s",

	// Window functions
	"SELECT %s, %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) as rn FROM %s.%s",
	"SELECT %s, %s, RANK() OVER (ORDER BY %s DESC) as rnk FROM %s.%s WHERE %s IS NOT NULL",

	// Subqueries
	"SELECT TOP %d * FROM %s.%s WHERE %s IN (SELECT TOP 10 %s FROM %s.%s)",
	"SELECT TOP %d * FROM %s.%s a WHERE EXISTS (SELECT 1 FROM %s.%s b WHERE a.%s = b.%s)",

	// Complex queries with multiple operations
	"WITH CTE AS (SELECT %s, %s FROM %s.%s WHERE %s IS NOT NULL) SELECT TOP %d * FROM CTE ORDER BY %s",
	"SELECT %s, %s FROM %s.%s WHERE %s IS NOT NULL ORDER BY %s",
}

func main() {
	fmt.Println("🚀 Starting DMV Populator for AdventureWorks2019")
	fmt.Println("📋 Target: Generate 350,000+ diverse queries to populate SQL Server DMVs")
	fmt.Println("⚡ Configuration: 12 workers, 25 minutes runtime")
	fmt.Println(strings.Repeat("=", 80))

	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Connect to database
	db := connectToDatabase()
	defer db.Close()

	// Configure SQL Server to prevent anonymization
	fmt.Println("📋 Configuring SQL Server to prevent query anonymization...")
	configureDatabase(db)

	// Check initial DMV state
	fmt.Println("🔍 Initial DMV state:")
	verifyDMVPopulation(db)

	// Initialize statistics
	var stats QueryStats
	stats.StartTime = time.Now()

	// Create channels for coordination
	queryChan := make(chan string, CONCURRENT_WORKERS*2)
	statsChan := make(chan WorkerStats, CONCURRENT_WORKERS)

	// Start worker goroutines
	fmt.Printf("🏭 Starting %d concurrent workers...\n", CONCURRENT_WORKERS)
	var wg sync.WaitGroup

	for i := 0; i < CONCURRENT_WORKERS; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runQueryWorker(db, workerID, queryChan, statsChan)
		}(i)
	}

	// Start query generator goroutine
	go generateQueries(queryChan, &stats)

	// Start stats collector goroutine
	go collectStats(statsChan, &stats)

	// Run for specified duration
	fmt.Printf("⏱️  Running for %d minutes to generate ~%d queries...\n", RUN_DURATION_MINUTES, TARGET_QUERY_COUNT)

	// Show progress for the specified duration
	ticker := time.NewTicker(30 * time.Second)
	timeout := time.After(time.Duration(RUN_DURATION_MINUTES) * time.Minute)

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(stats.StartTime)
			fmt.Printf("⏱️  Progress: %.1f minutes elapsed, %d queries generated\n",
				elapsed.Minutes(), stats.TotalQueries)
		case <-timeout:
			ticker.Stop()
			goto cleanup
		}
	}

cleanup:
	// Signal completion
	close(queryChan)

	// Wait for all workers to complete
	wg.Wait()
	close(statsChan)

	// Final statistics
	stats.EndTime = time.Now()
	stats.TotalExecutionTime = stats.EndTime.Sub(stats.StartTime)
	if stats.TotalExecutionTime.Seconds() > 0 {
		stats.QueriesPerSecond = float64(stats.TotalQueries) / stats.TotalExecutionTime.Seconds()
	}

	printFinalStats(&stats)
	exportStatsToCSV(&stats)

	// Verify DMV population
	fmt.Println("🔍 Final DMV state:")
	verifyDMVPopulation(db)

	fmt.Println("\n🎉 DMV Population completed successfully!")
}

func connectToDatabase() *sql.DB {
	connString := fmt.Sprintf("server=%s;port=%s;user id=%s;password=%s;database=%s;encrypt=disable",
		DB_SERVER, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

	fmt.Printf("🔗 Connecting to SQL Server %s/%s...\n", DB_SERVER, DB_NAME)
	db, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Error connecting to database: ", err.Error())
	}

	fmt.Printf("✅ Successfully connected to database: %s on server: %s\n", DB_NAME, DB_SERVER)
	log.Printf("📊 Database Connection Info - Server: %s, Port: %s, Database: %s, User: %s", DB_SERVER, DB_PORT, DB_NAME, DB_USER)
	return db
}

func configureDatabase(db *sql.DB) {
	_, err := db.Exec(PreventAnonymizationSQL)
	if err != nil {
		log.Printf("⚠️  Warning: Could not configure all SQL Server settings: %v", err)
	} else {
		fmt.Println("✅ SQL Server configured to prevent query anonymization")
	}
}

func generateQueries(queryChan chan<- string, stats *QueryStats) {
	queryCount := 0
	startTime := time.Now()

	for {
		// Check if we should stop based on time or query count
		if time.Since(startTime) > time.Duration(RUN_DURATION_MINUTES)*time.Minute {
			break
		}

		if queryCount >= TARGET_QUERY_COUNT {
			break
		}

		// Generate a random query
		query := generateRandomQuery()

		select {
		case queryChan <- query:
			queryCount++
			stats.TotalQueries++

			// Progress reporting
			if queryCount%PROGRESS_REPORT_INTERVAL == 0 {
				elapsed := time.Since(startTime)
				qps := float64(queryCount) / elapsed.Seconds()
				fmt.Printf("📊 Generated %d queries (%.1f q/s) - %.1f%% complete\n",
					queryCount, qps, float64(queryCount)/float64(TARGET_QUERY_COUNT)*100)
			}
		default:
			// Channel is full, wait a bit
			time.Sleep(10 * time.Millisecond)
		}
	}

	fmt.Printf("🎯 Query generation completed: %d queries generated\n", queryCount)
}

func generateRandomQuery() string {
	// Select random template
	template := QueryTemplates[rand.Intn(len(QueryTemplates))]

	// Select random table(s)
	table1 := AdventureWorksTables[rand.Intn(len(AdventureWorksTables))]
	table2 := AdventureWorksTables[rand.Intn(len(AdventureWorksTables))]

	// Select random columns
	col1 := table1.Columns[rand.Intn(len(table1.Columns))]
	col2 := table1.Columns[rand.Intn(len(table1.Columns))]
	col3 := table2.Columns[rand.Intn(len(table2.Columns))]

	// Generate random parameters
	topN := rand.Intn(100) + 1

	// Generate query based on template type
	var query string

	switch {
	case strings.Contains(template, "INNER JOIN"):
		query = fmt.Sprintf("SELECT TOP %d a.%s, b.%s FROM %s.%s a INNER JOIN %s.%s b ON a.%s = b.%s WHERE a.%s IS NOT NULL",
			topN, col1, col3, table1.Schema, table1.Name, table2.Schema, table2.Name, col1, col3, col2)
	case strings.Contains(template, "LEFT JOIN"):
		query = fmt.Sprintf("SELECT a.%s, b.%s, COUNT(*) FROM %s.%s a LEFT JOIN %s.%s b ON a.%s = b.%s GROUP BY a.%s, b.%s",
			col1, col3, table1.Schema, table1.Name, table2.Schema, table2.Name, col1, col3, col1, col3)
	case strings.Contains(template, "GROUP BY"):
		query = fmt.Sprintf("SELECT %s, COUNT(*) as cnt FROM %s.%s GROUP BY %s",
			col1, table1.Schema, table1.Name, col1)
	case strings.Contains(template, "ORDER BY"):
		query = fmt.Sprintf("SELECT DISTINCT TOP %d %s FROM %s.%s WHERE %s IS NOT NULL ORDER BY %s",
			topN, col1, table1.Schema, table1.Name, col2, col1)
	case strings.Contains(template, "ROW_NUMBER"):
		query = fmt.Sprintf("SELECT %s, %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) as rn FROM %s.%s",
			col1, col2, col1, col2, table1.Schema, table1.Name)
	case strings.Contains(template, "EXISTS"):
		query = fmt.Sprintf("SELECT TOP %d * FROM %s.%s a WHERE EXISTS (SELECT 1 FROM %s.%s b WHERE a.%s = b.%s)",
			topN, table1.Schema, table1.Name, table2.Schema, table2.Name, col1, col3)
	case strings.Contains(template, "IN (SELECT"):
		query = fmt.Sprintf("SELECT TOP %d * FROM %s.%s WHERE %s IN (SELECT TOP 10 %s FROM %s.%s)",
			topN, table1.Schema, table1.Name, col1, col3, table2.Schema, table2.Name)
	case strings.Contains(template, "CTE"):
		query = fmt.Sprintf("WITH CTE AS (SELECT %s, %s FROM %s.%s WHERE %s IS NOT NULL) SELECT TOP %d * FROM CTE ORDER BY %s",
			col1, col2, table1.Schema, table1.Name, col2, topN, col1)
	default:
		// Simple SELECT query
		query = fmt.Sprintf("SELECT TOP %d %s, %s FROM %s.%s WHERE %s IS NOT NULL",
			topN, col1, col2, table1.Schema, table1.Name, col1)
	}

	// Add query identifier comment to prevent anonymization
	queryID := fmt.Sprintf("DMV_POP_%d_%d", time.Now().UnixNano(), rand.Intn(100000))
	query = fmt.Sprintf("/* %s */ %s", queryID, query)

	return query
}

func runQueryWorker(db *sql.DB, workerID int, queryChan <-chan string, statsChan chan<- WorkerStats) {
	var workerStats WorkerStats
	workerStats.WorkerID = workerID

	fmt.Printf("👷 Worker %d started\n", workerID)

	for query := range queryChan {
		startTime := time.Now()

		// Log the query being executed
		log.Printf("🔍 Worker %d executing on database [%s]: %s", workerID, DB_NAME, query)

		// Execute query
		rows, err := db.Query(query)
		if err != nil {
			workerStats.QueriesFailed++
			workerStats.LastError = err.Error()
			// Don't log every error to avoid spam
			if rand.Intn(100) == 0 { // Log 1% of errors
				log.Printf("Worker %d error (sample): %v", workerID, err)
			}
		} else {
			// Consume all rows to ensure complete execution
			rowCount := 0
			for rows.Next() {
				rowCount++
				if rowCount > 1000 { // Limit row consumption to prevent memory issues
					break
				}
			}
			rows.Close()
			workerStats.QueriesSuccessful++
		}

		executionTime := time.Since(startTime)
		workerStats.ExecutionTime += executionTime
		workerStats.QueriesExecuted++

		// Report stats periodically
		if workerStats.QueriesExecuted%1000 == 0 {
			statsChan <- workerStats
		}

		// Small delay to prevent overwhelming the server
		if rand.Intn(20) == 0 { // 5% of queries have a small delay
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		}
	}

	// Final stats report
	statsChan <- workerStats
	fmt.Printf("👷 Worker %d completed: %d queries (%d successful, %d failed)\n",
		workerID, workerStats.QueriesExecuted, workerStats.QueriesSuccessful, workerStats.QueriesFailed)
}

func collectStats(statsChan <-chan WorkerStats, globalStats *QueryStats) {
	workerStats := make(map[int]*WorkerStats)

	for stats := range statsChan {
		workerStats[stats.WorkerID] = &stats

		// Update global stats
		globalStats.SuccessfulQueries = 0
		globalStats.FailedQueries = 0

		for _, ws := range workerStats {
			globalStats.SuccessfulQueries += ws.QueriesSuccessful
			globalStats.FailedQueries += ws.QueriesFailed
		}
	}
}

func printFinalStats(stats *QueryStats) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL DMV POPULATION STATISTICS")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("⏱️  Total Runtime: %v\n", stats.TotalExecutionTime)
	fmt.Printf("🎯 Total Queries Generated: %d\n", stats.TotalQueries)
	fmt.Printf("✅ Successful Queries: %d (%.1f%%)\n", stats.SuccessfulQueries,
		float64(stats.SuccessfulQueries)/float64(stats.TotalQueries)*100)
	fmt.Printf("❌ Failed Queries: %d (%.1f%%)\n", stats.FailedQueries,
		float64(stats.FailedQueries)/float64(stats.TotalQueries)*100)
	fmt.Printf("⚡ Average Queries/Second: %.2f\n", stats.QueriesPerSecond)

	if stats.SuccessfulQueries >= 300000 {
		fmt.Println("🎉 SUCCESS: Target of 3+ lakh queries achieved!")
	} else {
		fmt.Printf("⚠️  Note: Generated %d queries (target was %d)\n", stats.SuccessfulQueries, TARGET_QUERY_COUNT)
	}
}

func exportStatsToCSV(stats *QueryStats) {
	filename := fmt.Sprintf("dmv_population_stats_%s.csv", time.Now().Format("20060102_150405"))
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Error creating stats CSV: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"Metric", "Value"}
	writer.Write(header)

	// Write data
	records := [][]string{
		{"Total_Runtime_Minutes", fmt.Sprintf("%.2f", stats.TotalExecutionTime.Minutes())},
		{"Total_Queries_Generated", strconv.FormatInt(stats.TotalQueries, 10)},
		{"Successful_Queries", strconv.FormatInt(stats.SuccessfulQueries, 10)},
		{"Failed_Queries", strconv.FormatInt(stats.FailedQueries, 10)},
		{"Success_Rate_Percent", fmt.Sprintf("%.2f", float64(stats.SuccessfulQueries)/float64(stats.TotalQueries)*100)},
		{"Queries_Per_Second", fmt.Sprintf("%.2f", stats.QueriesPerSecond)},
		{"Start_Time", stats.StartTime.Format("2006-01-02 15:04:05")},
		{"End_Time", stats.EndTime.Format("2006-01-02 15:04:05")},
		{"Target_Achieved", fmt.Sprintf("%t", stats.SuccessfulQueries >= 300000)},
	}

	for _, record := range records {
		writer.Write(record)
	}

	fmt.Printf("📈 Statistics exported to %s\n", filename)
}

func verifyDMVPopulation(db *sql.DB) {
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "Query Stats Count",
			query: "SELECT COUNT(*) as query_count FROM sys.dm_exec_query_stats",
		},
		{
			name:  "Recent Queries (last hour)",
			query: "SELECT COUNT(*) as recent_queries FROM sys.dm_exec_query_stats WHERE last_execution_time >= DATEADD(hour, -1, SYSDATETIME())",
		},
		{
			name:  "Unique Query Hashes",
			query: "SELECT COUNT(DISTINCT query_hash) as unique_queries FROM sys.dm_exec_query_stats",
		},
		{
			name:  "Execution Plans Count",
			query: "SELECT COUNT(*) as plan_count FROM sys.dm_exec_query_stats WHERE query_plan_hash IS NOT NULL",
		},
	}

	for _, q := range queries {
		var count int
		err := db.QueryRow(q.query).Scan(&count)
		if err != nil {
			fmt.Printf("❌ Error checking %s: %v\n", q.name, err)
		} else {
			fmt.Printf("✅ %s: %d\n", q.name, count)
		}
	}
}
