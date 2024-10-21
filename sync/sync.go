package sync

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"gorm.io/gorm"
)

type SyncOptions struct {
	From              ReadableDb
	To                ReadWriteDb
	CollectionOptions map[string]CollectionOptions
	BeforeSync        func(state interface{}) error
	AfterSync         func(state interface{}) error
}

func SynchronizeDatabases(fromDb *gorm.DB, toDb *gorm.DB) error {
	collections, err := fromDb.Migrator().GetTables()
	if err != nil {
		return fmt.Errorf("error getting tables: %w", err)
	}

	fmt.Printf("Found %d tables to synchronize\n", len(collections))

	const (
		chunkSize = 500
		// Adjust the number of workers as needed
		numWorkers = 5
	)

	for _, table := range collections {
		if table == "logs" || table == "tb_reservation" {
			fmt.Printf("Skipping synchronization for table: %s\n", table)
			continue
		}

		fmt.Printf("Starting synchronization for table: %s\n", table)

		if err := createTableIfNotExists(fromDb, toDb, table); err != nil {
			return fmt.Errorf("error creating table %s: %w", table, err)
		}

		var sourceCount int64
		if err := fromDb.Table(table).Count(&sourceCount).Error; err != nil {
			return fmt.Errorf("error counting records in source %s: %w", table, err)
		}
		fmt.Printf("Records in source %s: [ %d ]\n", table, sourceCount)

		var (
			wg              sync.WaitGroup
			processedChunks int32
			tasks           = make(chan int, sourceCount/chunkSize)
		)

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for offset := range tasks {
					fmt.Printf("Worker %d (Goroutine ID: %d) processing chunk at offset %d\n", workerID, getGoroutineID(), offset)
					if err := processChunk(fromDb, toDb, table, offset, chunkSize); err != nil {
						fmt.Printf("Error processing chunk for %s at offset %d: %v\n", table, offset, err)
					}
					atomic.AddInt32(&processedChunks, 1)
				}
			}(w)
		}

		// Send tasks to the channel
		for offset := 0; offset < int(sourceCount); offset += chunkSize {
			tasks <- offset
		}
		close(tasks)

		wg.Wait()
		fmt.Printf("Completed synchronization for table: %s. Processed %d chunks.\n", table, processedChunks)
	}

	fmt.Println("Synchronization completed successfully 🎉 🎉 🎉")
	return nil
}

// Function to create the target table if it does not exist
func createTableIfNotExists(fromDb *gorm.DB, toDb *gorm.DB, table string) error {
	columnTypes, err := getColumnTypes(fromDb, table)
	if err != nil {
		return err
	}

	var columns []string
	for _, column := range columnTypes {
		columns = append(columns, fmt.Sprintf("`%s` %s", column.Name, column.Type))
	}

	createTableQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, strings.Join(columns, ", "))
	return toDb.Exec(createTableQuery).Error
}

// Updated getColumnTypes function to return ordered column information
func getColumnTypes(db *gorm.DB, table string) ([]ColumnInfo, error) {
	var columnTypes []ColumnInfo

	rows, err := db.Raw("SHOW COLUMNS FROM " + table).Rows()
	if err != nil {
		return nil, fmt.Errorf("error fetching column types for table %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var field, columnType, nullable, key, extra string
		var defaultValue *string // Use a pointer to handle NULL values

		if err := rows.Scan(&field, &columnType, &nullable, &key, &defaultValue, &extra); err != nil {
			return nil, fmt.Errorf("error scanning column types for table %s: %w", table, err)
		}

		sqlType := columnType
		if nullable == "YES" {
			sqlType += " NULL"
		} else {
			sqlType += " NOT NULL"
		}

		columnTypes = append(columnTypes, ColumnInfo{Name: field, Type: sqlType})
	}

	return columnTypes, nil
}

// Struct to hold column information
type ColumnInfo struct {
	Name string
	Type string
}

func processChunk(fromDb *gorm.DB, toDb *gorm.DB, table string, offset, chunkSize int) error {
	// If you want to see the time taken for each chunk, you can uncomment the following code
	// start := time.Now()
	// defer func() {
	// 	fmt.Printf("Chunk processing for %s (offset %d) took %v\n", table, offset, time.Since(start))
	// }()

	var sourceData []map[string]interface{}
	if err := fromDb.Table(table).Offset(offset).Limit(chunkSize).Find(&sourceData).Error; err != nil {
		return fmt.Errorf("error fetching data from %s: %w", table, err)
	}
	fmt.Printf("Fetched %d records <----- from source table : %s\n", len(sourceData), table)

	for i := range sourceData {
		transformRecord(sourceData[i])
	}

	if len(sourceData) == 0 {
		return nil
	}

	columns := make([]string, 0, len(sourceData[0]))
	for column := range sourceData[0] {
		if column == "desc" || column == "order" || column == "group" || column == "code" || column == "value" {
			columns = append(columns, "`"+column+"`") // Enclose with backticks
		} else {
			columns = append(columns, column)
		}
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", "))
	placeholders := make([]string, len(sourceData))
	values := make([]interface{}, 0, len(sourceData)*len(columns))

	for i, record := range sourceData {
		placeholders[i] = "(" + strings.Repeat("?,", len(columns)-1) + "?)"
		for _, column := range columns {
			values = append(values, record[column])
		}
	}

	query += strings.Join(placeholders, ", ")

	updateParts := make([]string, 0, len(columns))
	for _, column := range columns {
		updateParts = append(updateParts, fmt.Sprintf("%s = VALUES(%s)", column, column))
	}
	query += " ON DUPLICATE KEY UPDATE " + strings.Join(updateParts, ", ")

	result := toDb.Exec(query, values...)
	if result.Error != nil {
		return fmt.Errorf("failed to upsert data into target table: %s: %w", table, result.Error)
	}

	affected := result.RowsAffected
	fmt.Printf("Upserted %d rows -----> to target table : %s\n", affected, table)

	return nil
}

func transformRecord(record map[string]interface{}) {
	if _, ok := record["email"]; ok {
		record["email"] = "dev_hotel%v@movefast.xyz"
	}
	if _, ok := record["hotel_email"]; ok {
		record["hotel_email"] = "hotel_arch%v@movefast.xyz"
	}
	if _, ok := record["password"]; ok {
		record["password"] = "$2a$11$xLlTmByr6signynyPCofs.TG1SP06OX2WjL5pRtRk5SdgjqhMcRly"
	}
	if _, ok := record["hotel_phone"]; ok {
		record["hotel_phone"] = "080-2222-2222"
	}
	if _, ok := record["hotel_whatsapp"]; ok {
		record["hotel_whatsapp"] = "080-1111-1111"
	}
}

// Function to get the current goroutine ID
func getGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], true)
	for i := 0; i < n; i++ {
		if buf[i] == ' ' {
			return uint64(buf[i+1])
		}
	}
	return 0
}
