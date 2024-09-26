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
		fmt.Printf("Starting synchronization for table: %s\n", table)

		var sourceCount int64
		if err := fromDb.Table(table).Count(&sourceCount).Error; err != nil {
			return fmt.Errorf("error counting records in source %s: %w", table, err)
		}
		fmt.Printf("Records in source %s: %d\n", table, sourceCount)

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

func processChunk(fromDb *gorm.DB, toDb *gorm.DB, table string, offset, chunkSize int) error {
	// If you want to see the time taken for each chunk, you can uncomment the following code
	// start := time.Now()
	// defer func() {
	// 	fmt.Printf("Chunk processing for %s (offset %d) took %v\n", table, offset, time.Since(start))
	// }()

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
		columns = append(columns, column)
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
	query += " ON DUPLICATE KEY UPDATE "

	updates := make([]string, 0, len(columns))
	for _, column := range columns {
		if column != "id" {
			updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", column, column))
		}
	}
	query += strings.Join(updates, ", ")

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
	if id, ok := record["id"]; ok {
		if _, exists := record["email"]; exists {
			record["email"] = fmt.Sprintf("dev_hotel%v@movefast.xyz", id)
		}
		if _, exists := record["hotel_email"]; exists {
			record["hotel_email"] = fmt.Sprintf("hotel_arch%v@movefast.xyz", id)
		}
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
