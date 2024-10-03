package sync

import (
	"fmt"
	"strings"

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

	// Able to adjust this value based on your needs
	const chunkSize = 500

	for _, table := range collections {
		// Skip the specific table (e.g., "logs")
		if table == "logs" || table == "tb_reservation" {
			fmt.Printf("Skipping synchronization for table: %s\n", table)
			continue
		}

		fmt.Printf("Starting synchronization for table: %s\n", table)

		// Check if the target table exists, and create it if it doesn't
		if err := createTableIfNotExists(fromDb, toDb, table); err != nil {
			return fmt.Errorf("error creating table %s: %w", table, err)
		}

		var sourceCount int64
		if err := fromDb.Table(table).Count(&sourceCount).Error; err != nil {
			return fmt.Errorf("error counting records in source %s: %w", table, err)
		}
		fmt.Printf("Records in source %s: [ %d ]\n", table, sourceCount)

		// Process inserts and updates
		for offset := 0; offset < int(sourceCount); offset += chunkSize {
			// fmt.Printf("Processing chunk for %s: offset %d\n", table, offset)
			if err := processChunk(fromDb, toDb, table, offset, chunkSize); err != nil {
				return fmt.Errorf("error processing chunk for %s at offset %d: %w", table, offset, err)
			}
		}
		fmt.Printf("Completed synchronization for table: %s\n", table)
	}

	fmt.Println("Synchronization completed successfully 🎉 🎉 🎉")
	return nil
}

// Function to create the target table if it does not exist
func createTableIfNotExists(fromDb *gorm.DB, toDb *gorm.DB, table string) error {
	// Get the column types from the source table
	columnTypes, err := getColumnTypes(fromDb, table)
	if err != nil {
		return err
	}

	// Build the CREATE TABLE statement while maintaining the order of columns
	var columns []string
	for _, column := range columnTypes {
		// Use backticks for column names
		columns = append(columns, fmt.Sprintf("`%s` %s", column.Name, column.Type))
	}

	createTableQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, strings.Join(columns, ", "))
	return toDb.Exec(createTableQuery).Error
}

// Updated getColumnTypes function to return ordered column information
func getColumnTypes(db *gorm.DB, table string) ([]ColumnInfo, error) {
	var columnTypes []ColumnInfo

	// Query to get column information
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

		// Map the column type to a valid SQL type
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
	// Fetch chunk of data from source database
	var sourceData []map[string]interface{}
	if err := fromDb.Table(table).Offset(offset).Limit(chunkSize).Find(&sourceData).Error; err != nil {
		return fmt.Errorf("error fetching data from %s: %w", table, err)
	}
	fmt.Printf("Fetched %d records <----- from source table : %s\n", len(sourceData), table)

	// Transform source data
	for i := range sourceData {
		transformRecord(sourceData[i])
	}

	if len(sourceData) == 0 {
		return nil
	}

	// Prepare the upsert query
	columns := make([]string, 0, len(sourceData[0]))
	for column := range sourceData[0] {
		// Use backticks for the column names if they are "desc" or "order"
		if column == "desc" || column == "order" || column == "group" || column == "code" || column == "value" {
			columns = append(columns, "`"+column+"`") // Enclose with backticks
		} else {
			columns = append(columns, column)
		}
	}

	// Prepare the insert statement
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

	// Prepare the ON DUPLICATE KEY UPDATE part
	updateParts := make([]string, 0, len(columns))
	for _, column := range columns {
		updateParts = append(updateParts, fmt.Sprintf("%s = VALUES(%s)", column, column))
	}
	query += " ON DUPLICATE KEY UPDATE " + strings.Join(updateParts, ", ")

	// Execute the upsert query
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
