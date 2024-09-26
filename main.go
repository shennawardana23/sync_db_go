package main

import (
	"fmt"
	"log"
	"os"
	"sync_db_go/sync"

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
	// Database connection for production
	dsnProduction := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=True",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)

	dbProduction, err := gorm.Open(mysql.Open(dsnProduction), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to the production database:", err)
	}

	// Database connection for staging
	dsnStaging := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=True",
		os.Getenv("DB_USER_STAGING"),
		os.Getenv("DB_PASSWORD_STAGING"),
		os.Getenv("DB_HOST_STAGING"),
		os.Getenv("DB_PORT_STAGING"),
		os.Getenv("DB_NAME_STAGING"),
	)

	dbStaging, err := gorm.Open(mysql.Open(dsnStaging), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to the staging database:", err)
	}

	// Database connection for local
	dsnLocal := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		os.Getenv("DB_USER_LOCAL"),
		os.Getenv("DB_PASSWORD_LOCAL"),
		os.Getenv("DB_HOST_LOCAL"),
		os.Getenv("DB_PORT_LOCAL"),
		os.Getenv("DB_NAME_LOCAL"),
	)

	dbLocal, err := gorm.Open(mysql.Open(dsnLocal), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to the local database:", err)
	}

	// Perform database synchronization
	err = sync.SyncDatabases(dbProduction, dbStaging, dbLocal)
	if err != nil {
		log.Fatal("Error during synchronization:", err)
	}
	fmt.Println("Synchronization completed successfully.")
}
