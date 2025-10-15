# huedb

A basic Go project scaffold ready for a database-backed side project.

## Getting Started

1. Install Go (1.22+ recommended)
2. Install PostgreSQL (or update the code for your DB)
3. Configure the DB connection string in `main.go`
4. Download dependencies:
   ```sh
   go mod tidy
   ```
5. Run the app:
   ```sh
   go run main.go
   ```

## Dependencies

- github.com/lib/pq (PostgreSQL driver)

Replace DB settings as needed for your database of choice.

