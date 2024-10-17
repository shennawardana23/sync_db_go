# sync_db_go

## DB Synch

- Make synchronize transfer data from production to staging

When  run the `SynchronizeDatabases` function, it performs the following steps:

1. **Fetch Data**: It retrieves all records from the `db_staging` for each table.

2. **Insert New Data**: After clearing the local table, it inserts all the records fetched from `db_staging` into `db_local`.

## Key Points

- **Updated**: The process effectively updated all records in `db_local` with those from `db_staging`. If it exists, it compares the existing record with the new one and updates only if there are differences.
  
- **Transactions**: The insertion is done in a single transaction for each table, meaning that if any error occurs during the insertion, the entire operation for that table will fail, and no records will be inserted. This ensures data integrity.

## Conclusion

If all data in `db_local` is the same as in `db_staging`, the synchronization process will still delete and then reinsert the data, which is not the most efficient approach. If you want to avoid unnecessary operations, you could implement a check to compare the data before deciding to delete and insert. This would require additional logic to determine if the records are identical.

## Criteria

- Iterates through each record from the source database.
- Applies transformations to the record.
- Checks if the record exists in the target database:
- Avoid memory leak with data larges
- Transaction per table: Wraps operations for each table in a transaction, which can help manage consistency and potentially reduce lock duration.
- Short delay between chunks: Allows other transactions to proceed, potentially reducing contention.
- Timeout for bulk updates: Prevents updates from hanging indefinitely.
- Can mitigate locking and deadlock issues, but the effectiveness may vary depending on your specific database system and workload. Need to fine-tune these approaches based on particular use case and database performance characteristics.

## Approach

- SyncOptions struct: Defines options for the synchronization process, including source and destination databases, collection options, and hooks for before and after sync.
- SyncDatabase function: A high-level function that executes the sync process based on the provided options. It calls the BeforeSync and AfterSync hooks if they're defined.
- SynchronizeDatabases function: The main function that orchestrates the synchronization process between two GORM database connections. It iterates through all tables and processes them in chunks.
- processChunk function: Handles the synchronization of a chunk of data for a specific table. It fetches data from the source, transforms it, and then performs bulk inserts or updates in the destination database.
- bulkUpdate function: Performs a bulk update operation using a single SQL query with a CASE statement for efficiency.
- chunkedDeletion function: Deletes records from the destination database that no longer exist in the source database, processing in chunks to manage memory usage.
- transformRecord function: Applies transformations to individual records, such as anonymizing email addresses and standardizing certain fields.

**The code implements a robust synchronization process that**:
[ ] Handles large datasets by processing data in chunks
[ ] Performs efficient bulk insert and update operations
[ ] Manages deletions of obsolete records
[ ] Applies data transformations
[ ] Uses GORM for database operations, making it compatible with various database systems
[ ] This implementation is designed to be efficient and scalable, suitable for synchronizing large databases while managing memory usage and performance.

## Run Migration

```bash
> make run
```

# Synchronization Process Diagram

```mermaid
graph TD;
    A[db_staging - Source DB] -->|Fetch Data| B[Fetch Data]
    B --> D[Transform Data]
    D --> E[Insert/Update]
    E --> F[Delete Obsolete Records]
    A -->|Check Existence| C[Check Existence]
    C --> E
    A -->|db_local - Target DB| D
````

---
