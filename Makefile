migration?=staging

run: ## -- Run the application
	@go mod tidy
	@go run main.go

run: export MIGRATION_FROM=$(migration)