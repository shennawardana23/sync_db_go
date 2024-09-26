package sync

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

type SyncOptions struct {
	From              ReadableDb
	To                ReadWriteDb
	CollectionOptions map[string]CollectionOptions
	BeforeSync        func(state interface{}) error
	AfterSync         func(state interface{}) error
}

func SyncDatabase(options SyncOptions) error {
	state := make(map[string]interface{})

	if options.BeforeSync != nil {
		if err := options.BeforeSync(state); err != nil {
			return err
		}
	}

	fmt.Println("Starting sync...")
	// Add your sync logic here if necessary

	if options.AfterSync != nil {
		if err := options.AfterSync(state); err != nil {
			return err
		}
	}

	return nil
}

func SynchronizeDatabases(fromDb *gorm.DB, toDb *gorm.DB) error {
	collections, err := fromDb.Migrator().GetTables()
	if err != nil {
		return err
	}

	const chunkSize = 1000 // Adjust this value based on your needs and memory constraints

	for _, table := range collections {
		var totalRecords int64
		if err := fromDb.Table(table).Count(&totalRecords).Error; err != nil {
			return fmt.Errorf("error counting records in %s: %w", table, err)
		}

		// Start a transaction for each table
		err := toDb.Transaction(func(tx *gorm.DB) error {
			for offset := 0; offset < int(totalRecords); offset += chunkSize {
				if err := processChunk(fromDb, tx, table, offset, chunkSize); err != nil {
					return err
				}
			}

			if err := chunkedDeletion(fromDb, tx, table, chunkSize); err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func processChunk(fromDb *gorm.DB, toDb *gorm.DB, table string, offset, chunkSize int) error {
	// Fetch chunk of data from source database
	var sourceData []map[string]interface{}
	if err := fromDb.Table(table).Offset(offset).Limit(chunkSize).Find(&sourceData).Error; err != nil {
		return fmt.Errorf("error fetching data from %s: %w", table, err)
	}

	// Transform source data
	for i := range sourceData {
		transformRecord(sourceData[i])
	}

	// Extract IDs from the current chunk
	var chunkIDs []interface{}
	for _, record := range sourceData {
		chunkIDs = append(chunkIDs, record["id"])
	}

	// Fetch corresponding records from target database
	var targetData []map[string]interface{}
	if err := toDb.Table(table).Where("id IN ?", chunkIDs).Find(&targetData).Error; err != nil {
		return fmt.Errorf("error fetching data from target %s: %w", table, err)
	}

	// Prepare data for operations
	toInsert := []map[string]interface{}{}
	toUpdate := []map[string]interface{}{}
	existingIDs := make(map[interface{}]bool)

	for _, targetRecord := range targetData {
		existingIDs[targetRecord["id"]] = true
	}

	for _, sourceRecord := range sourceData {
		id := sourceRecord["id"]
		if !existingIDs[id] {
			toInsert = append(toInsert, sourceRecord)
		} else {
			toUpdate = append(toUpdate, sourceRecord)
		}
	}

	// Perform bulk insert
	if len(toInsert) > 0 {
		if err := toDb.Table(table).Create(toInsert).Error; err != nil {
			return fmt.Errorf("failed to bulk insert into %s: %w", table, err)
		}
	}

	// Perform bulk update using raw SQL
	if len(toUpdate) > 0 {
		if err := bulkUpdate(toDb, table, toUpdate); err != nil {
			return fmt.Errorf("failed to bulk update %s: %w", table, err)
		}
	}

	// Add a short delay between chunks to allow other transactions to proceed
	time.Sleep(10 * time.Millisecond)

	return nil
}

func bulkUpdate(db *gorm.DB, table string, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// Get column names from the first record
	var columns []string
	for column := range records[0] {
		if column != "id" {
			columns = append(columns, column)
		}
	}

	// Construct the SQL query
	query := fmt.Sprintf("UPDATE %s SET ", table)
	for i, column := range columns {
		if i > 0 {
			query += ", "
		}
		query += fmt.Sprintf("%s = CASE id ", column)
		for range records {
			query += "WHEN ? THEN ? "
		}
		query += "ELSE " + column + " END"
	}

	// Add WHERE clause
	query += " WHERE id IN (" + strings.Repeat("?,", len(records)-1) + "?)"

	// Prepare the arguments for the statement
	args := []interface{}{}
	for _, column := range columns {
		for _, record := range records {
			args = append(args, record["id"], record[column])
		}
	}
	for _, record := range records {
		args = append(args, record["id"])
	}

	// Execute the prepared statement with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result := db.WithContext(ctx).Exec(query, args...)
	if result.Error != nil {
		return fmt.Errorf("failed to execute bulk update: %w", result.Error)
	}

	return nil
}

func chunkedDeletion(fromDb *gorm.DB, toDb *gorm.DB, table string, chunkSize int) error {
	var offset int64

	// Prepare the deletion statement
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id NOT IN (SELECT id FROM (?) AS temp)", table)

	for {
		var sourceIDs []interface{}
		if err := fromDb.Table(table).Select("id").Offset(int(offset)).Limit(chunkSize).Pluck("id", &sourceIDs).Error; err != nil {
			return fmt.Errorf("error fetching source IDs from %s: %w", table, err)
		}

		if len(sourceIDs) == 0 {
			break // No more IDs to process
		}

		// Construct the subquery for the current chunk
		subquery := fromDb.Table(table).Select("id").Where("id IN (?)", sourceIDs)

		// Execute the prepared deletion statement
		if err := toDb.Exec(deleteQuery, subquery).Error; err != nil {
			return fmt.Errorf("failed to delete obsolete records from %s: %w", table, err)
		}

		offset += int64(len(sourceIDs))
	}

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

// SyncDatabases synchronizes data between production, staging, and local databases
func SyncDatabases(dbProduction, dbStaging, dbLocal *gorm.DB) error {
	// Synchronize data from production to staging
	err := SynchronizeDatabases(dbProduction, dbStaging)
	if err != nil {
		return fmt.Errorf("error synchronizing production to staging: %w", err)
	}

	// Synchronize data from staging to local
	err = SynchronizeDatabases(dbStaging, dbLocal)
	if err != nil {
		return fmt.Errorf("error synchronizing staging to local: %w", err)
	}

	return nil
}
