package db

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/shopspring/decimal"
)

var Pool *pgxpool.Pool
var columns []string

// DatabaseConfig represents the structure of the YAML file
type DatabaseConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`
	LogLevel string `yaml:"logLevel"`
	NATSURL  string `yaml:"nats_url"`
}

// CustomLogger is a custom logger that satisfies the pgx.Logger interface
type CustomLogger struct {
	logger *log.Logger
	level  pgx.LogLevel
}

// Log implements the pgx.Logger interface
func (cl *CustomLogger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	if level <= cl.level {
		cl.logger.Printf("%s: %s %s\n", level, msg, data)
	}
}

func InitDB(config *DatabaseConfig) error {
	ctx := context.Background()

	// Map log level values from the config file to pgx.LogLevel constants
	logLevelMapping := map[string]pgx.LogLevel{
		"debug": pgx.LogLevelDebug,
		"info":  pgx.LogLevelInfo,
		"warn":  pgx.LogLevelWarn,
		"error": pgx.LogLevelError,
		// Add other mappings as needed
	}

	// Get the log level value from the config file
	configLogLevel, ok := logLevelMapping[config.LogLevel]
	if !ok {
		// Default to LogLevelError or handle the error accordingly
		configLogLevel = pgx.LogLevelError
	}

	// Create a connection pool configuration
	poolConfig, err := pgxpool.ParseConfig(buildConnString(config))
	if err != nil {
		return fmt.Errorf("error parsing connection string: %v", err)
	}

	// Create a custom logger with the desired log level
	customLogger := &CustomLogger{
		logger: log.New(os.Stdout, "pgxpool:", log.LstdFlags),
		level:  configLogLevel,
	}

	// Set the custom logger for the connection pool
	poolConfig.ConnConfig.Logger = customLogger

	// Create a connection pool
	Pool, err = pgxpool.ConnectConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("unable to connect to the database: %v", err)
	}

	return nil
}

// buildConnString builds the PostgreSQL connection string from the DatabaseConfig
func buildConnString(config *DatabaseConfig) string {
	return fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=%s",
		config.User, config.Password, config.Host, config.Port, config.DBName, config.SSLMode)
}

func FetchDataFromTable(query string, wg *sync.WaitGroup) ([]map[string]interface{}, error) {
	//inicio := time.Now()

	// Acquire a connection from the pool
	conn, err := Pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	// Execute the query
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get information about the columns
	colDescs := rows.FieldDescriptions()

	// Extract column names from FieldDescriptions
	columns := make([]string, len(colDescs))
	for i, colDesc := range colDescs {
		columns[i] = string(colDesc.Name)
	}

	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		columnPointers := make([]interface{}, len(columns))
		columnData := make([]interface{}, len(columns))

		for i := range columnData {
			columnPointers[i] = &columnData[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		entry := make(map[string]interface{})

		for i, colName := range columns {
			val := columnData[i]

			if b, ok := val.([]byte); ok {
				entry[colName] = string(b)
			} else {
				entry[colName] = val
			}
		}

		result = append(result, entry)
	}

	/*
		fim := time.Now()
		// Calculate the time difference
		tempoDecorrido := fim.Sub(inicio)

		// Display the elapsed time
		fmt.Printf("Select took %s to execute\n", tempoDecorrido)*/

	result = formataToNativeType(result)

	return result, nil
}

func IsPoolConnected(pool *pgxpool.Pool) bool {
	ctx := context.Background()
	err := pool.Ping(ctx)
	return err == nil
}

// Function to generate a random number within a given range
func generateRandomNumber(min, max int) int {
	source := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(source)
	return rnd.Intn(max-min+1) + min
}

func generateUniqueTempTableName(table string) string {
	uniqueID := uuid.New()
	// Remove hyphens from the UUID string
	cleanedUUID := strings.ReplaceAll(uniqueID.String(), "-", "")
	return fmt.Sprintf("temp_%s_%s", table, cleanedUUID)
}

// InsertBulkData inserts data in bulk into a PostgreSQL table with ON CONFLICT UPDATE clause
func InsertBulkData(ctx context.Context, data []map[string]interface{}, table string, primaryKey []string, timeout time.Duration) error {
	if len(data) == 0 {
		return nil
	}

	// Create a new context with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	columns := getColumns(data)

	// Format timestamps before inserting
	data = formatTimestamps(data, columns)

	data = formatToBinaryData(data, columns)

	// Begin the transaction
	tx, err := Pool.Begin(ctxWithTimeout)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctxWithTimeout)

	tempTable := generateUniqueTempTableName(table)

	// Create a temporary table
	_, err = tx.Exec(ctxWithTimeout, fmt.Sprintf("CREATE TEMPORARY TABLE %s AS TABLE %s WITH NO DATA", tempTable, table))
	if err != nil {
		return err
	}

	dataToInsert := newMapCopyFromSource(data, columns)

	// Copy data into the temporary table using the COPY command
	_, err = tx.CopyFrom(ctxWithTimeout, pgx.Identifier{tempTable}, columns, dataToInsert)

	if err != nil {
		log.Printf("Error during COPY operation: %v", err)

		// Extract and print information from PgError
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("Error details:\n%s\n", pgErr.Error())
			// ... (rest of the error details extraction)
		}
		return err
	}

	// Construct the final INSERT statement with ON CONFLICT UPDATE
	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) SELECT DISTINCT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(columns, ", "),
		tempTable,
		strings.Join(primaryKey, ", "),
		buildUpdateValuesWithExcluded(columns, primaryKey),
	)

	// Execute the final INSERT statement
	_, err = tx.Exec(ctxWithTimeout, insertStmt)
	if err != nil {
		return err
	}

	// Commit the transaction
	err = tx.Commit(ctxWithTimeout)
	if err != nil {
		return err
	}

	return nil
}

// ...

func buildUpdateValuesWithExcluded(columns []string, primaryKey []string) string {
	var updateAssignments []string
	for _, col := range columns {
		// Exclude primary key columns from the update
		if !contains(primaryKey, col) {
			updateAssignments = append(updateAssignments, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}
	return strings.Join(updateAssignments, ", ")
}

func formataToNativeType(data []map[string]interface{}) []map[string]interface{} {
	newData := make([]map[string]interface{}, len(data))

	for i, row := range data {
		newRow := make(map[string]interface{}, len(row))
		for col, value := range row {
			switch v := value.(type) {
			case pgtype.Timestamptz:
				if v.Status == pgtype.Present {
					newRow[col] = v.Time
				}
			case pgtype.Float8:
				if v.Status == pgtype.Present {
					newRow[col] = v.Float
				}
			case pgtype.Int4:
				if v.Status == pgtype.Present {
					newRow[col] = int(v.Int)
				}
			case pgtype.Bool:
				if v.Status == pgtype.Present {
					newRow[col] = v.Bool
				}
			case pgtype.Text:
				if v.Status == pgtype.Present {
					newRow[col] = v.String
				}
			case pgtype.Numeric:
				if v.Status == pgtype.Present {
					// Convert the Numeric value to a decimal.Decimal
					decimalVal, err := v.Value()
					if err != nil {
						// Handle the error
						fmt.Printf("Error converting Numeric to decimal.Decimal: %v\n", err)
						continue
					}

					// Convert driver.Value (string) to decimal.Decimal
					decimalValue, err := decimal.NewFromString(decimalVal.(string))
					if err != nil {
						// Handle the error
						fmt.Printf("Error converting string to decimal.Decimal: %v\n", err)
						continue
					}

					// Convert decimal.Decimal to float64
					floatVal, _ := decimalValue.Float64()
					newRow[col] = floatVal
				}

			default:
				newRow[col] = value
			}
		}
		newData[i] = newRow
	}

	return newData
}

func formatToBinaryData(data []map[string]interface{}, columnOrder []string) []map[string]interface{} {
	newData := make([]map[string]interface{}, len(data))

	for i, row := range data {
		newRow := make(map[string]interface{}, len(row))
		for _, col := range columnOrder {
			switch v := row[col].(type) {
			case time.Time:
				newRow[col] = pgtype.Timestamptz{Time: v, Status: pgtype.Present}
			case float64:
				newRow[col] = pgtype.Float8{Float: v, Status: pgtype.Present}
			case int:
				newRow[col] = int32(v)
			case bool:
				//newRow[col] = boolToInt(v)
				newRow[col] = pgtype.Bool{Bool: v, Status: pgtype.Present}
			case string:
				if col == "time" {
					// Parse the string as time
					t, err := time.Parse(time.RFC3339, v)
					if err != nil {
						// Handle the error
						fmt.Printf("Error parsing time: %v\n", err)
						continue
					}
					newRow[col] = pgtype.Timestamptz{Time: t, Status: pgtype.Present}
				} else {
					newRow[col] = pgtype.Text{String: v, Status: pgtype.Present}
				}
			default:
				newRow[col] = row[col]
			}
		}
		newData[i] = newRow
	}

	// Print the values for debugging
	/*for _, row := range newData {
		fmt.Printf("Row: %+v\n", row)
	}*/

	return newData
}

// Helper function to convert bool to int
func boolToInt(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// formatTimestamps formats timestamp values in the data to strings
func formatTimestamps(data []map[string]interface{}, columnOrder []string) []map[string]interface{} {
	newData := make([]map[string]interface{}, len(data))

	for i, row := range data {
		newRow := make(map[string]interface{}, len(row))
		for _, col := range columnOrder {
			if timestamp, ok := row[col].(time.Time); ok {
				newRow[col] = timestamp.Format(time.RFC3339)
			} else if strNum, ok := row[col].(string); ok {
				// Try to convert string number to float64
				if num, err := strconv.ParseFloat(strNum, 64); err == nil {
					newRow[col] = num
				} else {
					// If conversion fails, keep the original string value
					newRow[col] = row[col]
				}
			} else {
				newRow[col] = row[col]
			}
		}
		newData[i] = newRow
	}

	return newData
}

// getColumns returns the columns as a slice of strings
func getColumns(data []map[string]interface{}) []string {
	if len(data) == 0 {
		return nil
	}

	// Use the keys of the first map as the order of columns
	columns := make([]string, 0, len(data[0]))
	for column := range data[0] {
		columns = append(columns, column)
	}

	return columns
}

// buildUpdateValues constructs the SET clause for ON CONFLICT UPDATE
func buildUpdateValues(primaryKey []string, updateAssignments []string) string {
	updateClause := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s",
		strings.Join(primaryKey, ", "),
		strings.Join(updateAssignments, ", "),
	)
	return updateClause
}

// contains checks if a string is present in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// mapCopyFromSource is an implementation of pgx.CopyFromSource for a slice of maps
type mapCopyFromSource struct {
	data    []map[string]interface{}
	pos     int
	columns []string // Explicitly define the order of columns
}

// newMapCopyFromSource creates a new mapCopyFromSource
func newMapCopyFromSource(data []map[string]interface{}, columnsOrder []string) *mapCopyFromSource {
	return &mapCopyFromSource{
		data:    data,
		pos:     0,
		columns: columnsOrder,
	}
}

// Next implements the pgx.CopyFromSource interface
func (m *mapCopyFromSource) Next() bool {
	return m.pos < len(m.data)
}

// Values implements the pgx.CopyFromSource interface
func (m *mapCopyFromSource) Values() ([]interface{}, error) {
	if !m.Next() {
		return nil, io.EOF
	}

	values := make([]interface{}, len(m.columns))
	for i, col := range m.columns {
		values[i] = m.data[m.pos][col]
	}

	m.pos++
	return values, nil
}

// Err implements the pgx.CopyFromSource interface
func (m *mapCopyFromSource) Err() error {
	return nil
}
