package main

import (
	"fmt"
	"log"
	"os"
	"sync_db_go/sync"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func init() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func main() {
	var (
		dbProduction, dbStaging, dbLocal *gorm.DB
		err                              error
		migrationFrom                    = os.Getenv("MIGRATION_FROM")
	)

	// Perform database synchronization
	if migrationFrom == "production" {
		fmt.Println("Starting synchronization from production to staging 🚀 🚀 🚀")
		totalStart := time.Now()

		// Connect to production database
		dbProduction, err = connectToDatabase("production")
		if err != nil {
			log.Printf("Warning: Failed to connect to the production database: %v", err)
		}

		// Connect to staging database
		dbStaging, err = connectToDatabase("staging")
		if err != nil {
			log.Fatal("Failed to connect to the staging database:", err)
		}

		// Synchronize data from production to staging
		err = sync.SynchronizeDatabases(dbProduction, dbStaging)
		if err != nil {
			log.Fatal("error synchronizing production to staging:", err)
		}

		fmt.Printf("Total synchronization time from %s: %v\n 🕒", migrationFrom, time.Since(totalStart))

	} else if migrationFrom == "staging" {
		fmt.Println("Starting synchronization from staging to local 🚀 🚀 🚀")
		totalStart := time.Now()

		// Connect to staging database
		dbStaging, err = connectToDatabase("staging")
		if err != nil {
			log.Fatal("Failed to connect to the staging database:", err)
		}

		// Connect to local database
		dbLocal, err = connectToDatabase("local")
		if err != nil {
			log.Fatal("Failed to connect to the local database:", err)
		}

		// Synchronize data from staging to local
		err = sync.SynchronizeDatabases(dbStaging, dbLocal)
		if err != nil {
			log.Fatal("error synchronizing staging to local:", err)
		}

		fmt.Printf("Total synchronization time 🕒 from %s: %v\n", migrationFrom, time.Since(totalStart))
	}
}

func connectToDatabase(env string) (*gorm.DB, error) {
	var dsn string
	switch env {
	case "production":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=True",
			os.Getenv("DB_USER"),
			os.Getenv("DB_PASSWORD"),
			os.Getenv("DB_HOST"),
			os.Getenv("DB_PORT"),
			os.Getenv("DB_NAME"))
	case "staging":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=True",
			os.Getenv("DB_USER_STAGING"),
			os.Getenv("DB_PASSWORD_STAGING"),
			os.Getenv("DB_HOST_STAGING"),
			os.Getenv("DB_PORT_STAGING"),
			os.Getenv("DB_NAME_STAGING"))
	case "local":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			os.Getenv("DB_USER_LOCAL"),
			os.Getenv("DB_PASSWORD_LOCAL"),
			os.Getenv("DB_HOST_LOCAL"),
			os.Getenv("DB_PORT_LOCAL"),
			os.Getenv("DB_NAME_LOCAL"))
	default:
		return nil, fmt.Errorf("unknown environment: %s", env)
	}

	if dsn == ":@tcp(:)/?parseTime=True" {
		return nil, fmt.Errorf("empty database credentials for %s", env)
	}

	return gorm.Open(mysql.Open(dsn), &gorm.Config{})
}
