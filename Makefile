migration?=staging

run: ## -- Run the application
	@echo "Synchronizing from $(migration)"
	go mod tidy
	go run main.go

run: export MIGRATION_FROM=$(migration)