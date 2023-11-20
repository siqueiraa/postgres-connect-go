# PostgreSQL Connection and Data Manipulation in Go

This Go package provides a convenient way to connect to a PostgreSQL database, fetch data from a table, and insert data in bulk. The package utilizes the `pgx` library for database interactions.

## Usage

### 1. Connection Setup

1. **Clone this repository:**

    ```bash
    git clone https://github.com/siqueiraa/postgres-connect-go.git
    ```

2. **Create a YAML configuration file (`config.yaml`) with your PostgreSQL connection details:**

    ```yaml
    user: your_username
    password: your_password
    host: localhost
    port: 5432
    dbname: your_database
    sslmode: disable
    logLevel: debug
    ```

### 2. Initialize the Database Connection

In your Go code, import the `db` package and initialize the database connection:

```go
package main

import (
	"log"
	"github.com/siqueiraa/postgres-connect-go/db"
)

func main() {
	err := db.InitDB()
	if err != nil {
		log.Fatal("Error initializing the database:", err)
	}
	defer db.Pool.Close()
}

```

### 3. Fetch Data from a Table
In your Go code, use the following snippet to fetch data from a PostgreSQL table:

```
package main

import (
	"fmt"
	"github.com/siqueiraa/postgres-connect-go/db"
)

func main() {
    // Define your SQL query
	query := "SELECT * FROM your_table"
    
    // Fetch data from the table
	result, err := db.FetchDataFromTable(query, nil)
	if err != nil {
		fmt.Println("Error fetching data:", err)
		return
	}

	fmt.Println("Fetched data:", result)
}

```

### 4. Insert Bulk Data into a Table
In your Go code, use the following snippet to insert bulk data into a PostgreSQL table:

```
package main

import (
	"fmt"
	"github.com/siqueiraa/postgres-connect-go/db"
)

func main() {
    // Your data to be inserted
	data := []map[string]interface{}{
		{"column1": value1, "column2": value2},
		// Add more rows as needed
	}

    // Specify the target table and primary key columns
	tableName := "your_table"
	primaryKey := []string{"column1"}

    // Insert bulk data into the table
	err := db.InsertBulkData(data, tableName, primaryKey)
	if err != nil {
		fmt.Println("Error inserting bulk data:", err)
		return
	}

	fmt.Println("Bulk data inserted successfully.")
}

```

### Note
Ensure that your PostgreSQL server is running and accessible.
Modify the connection details and queries according to your database and table structure.
For more detailed information, refer to the https://pkg.go.dev/github.com/jackc/pgx/v4
