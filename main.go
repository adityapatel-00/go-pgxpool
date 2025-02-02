package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
)

// DBConfig holds all database configuration parameters
type DBConfig struct {
	Host              string `mapstructure:"PG_HOST"`
	Port              int    `mapstructure:"PG_PORT"`
	UserName          string `mapstructure:"PG_USERNAME"`
	Password          string `mapstructure:"PG_PASSWORD"`
	DBName            string `mapstructure:"PG_DBNAME"`
	MaxConns          int32
	MinConns          int32
	MaxConnLifeTime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

type App struct {
	DBClient *pgxpool.Pool
}

func main() {
	// Create a root context with cancellation
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize database configuration from environment variables
	dbConfig, err := LoadConfig(".env") // Change for yaml or json. ex: config.yaml
	if err != nil {
		slog.Info("Error loading config", slog.String("error=", err.Error()))
	}
	dbConfig.MaxConns = 10
	dbConfig.MinConns = 2                        // Minimum connections in the pool, default is 0
	dbConfig.MaxConnLifeTime = 30 * time.Minute  // Maximum connection lifetime, default is one hour
	dbConfig.MaxConnIdleTime = 10 * time.Minute  // Maximum idle time, default is 30 minutes
	dbConfig.HealthCheckPeriod = 2 * time.Minute // Health check frequency, default is 60 seconds

	slog.Info("config", slog.Any("c=", dbConfig))

	// Create the connection pool
	db, err := NewPg(rootCtx, dbConfig, WithPgxConfig(dbConfig))
	if err != nil {
		slog.Error("Error connecting to database", slog.String("error", err.Error()))
		panic(err)
	}
	defer db.Close()

	app := &App{
		DBClient: db,
	}
	slog.Info("Application started successfully!")

	// Do some operations
	// V1: Acquiring explicit connection
	err = app.DoExplicitConnectionOperations(rootCtx)
	if err != nil {
		slog.Error("Error during explicit connection operations", slog.String("error", err.Error()))
	}

	// V2: Using pool directly
	err = app.DoDirectPoolOperations(rootCtx)
	if err != nil {
		slog.Error("Error during direct pool operations", slog.String("error", err.Error()))
	}

	// Monitor pool stats
	app.monitorPoolStats()
}

func LoadConfig(configFile string) (*DBConfig, error) {
	var cfg DBConfig

	// Set file name for environment configuration
	viper.SetConfigFile(configFile)
	viper.AutomaticEnv() // Read environment variables

	// Read the configuration file
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	// Unmarshal into DBConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Ensure singleton pattern for connection configuration
var pgOnce sync.Once

// Create a pgx connection config from DBConfig
func WithPgxConfig(dbConfig *DBConfig) *pgx.ConnConfig {
	// Create the dsn string
	connString := strings.TrimSpace(fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%d",
		dbConfig.UserName, dbConfig.Password, dbConfig.DBName,
		dbConfig.Host, dbConfig.Port))

	config, err := pgx.ParseConfig(connString)
	if err != nil {
		slog.Error("Error parsing connection config", slog.String("error", err.Error()))
		panic(err)
	}

	return config
}

// Create a new connection pool with the provided configuration
func NewPg(ctx context.Context, dbConfig *DBConfig, pgxConfig *pgx.ConnConfig) (*pgxpool.Pool, error) {
	// Parse the pool configuration from connection string
	config, err := pgxpool.ParseConfig(pgxConfig.ConnString())
	if err != nil {
		slog.Error("Error parsing pool config", slog.String("error", err.Error()))
		return nil, err
	}

	// Apply pool-specific configurations
	config.MaxConns = dbConfig.MaxConns
	config.MinConns = dbConfig.MinConns
	config.MaxConnLifetime = dbConfig.MaxConnLifeTime
	config.MaxConnIdleTime = dbConfig.MaxConnIdleTime
	config.HealthCheckPeriod = dbConfig.HealthCheckPeriod

	// Initialize the pool with singleton pattern
	var db *pgxpool.Pool
	pgOnce.Do(func() {
		db, err = pgxpool.NewWithConfig(ctx, config)
	})

	// Verify the connection
	if err = db.Ping(ctx); err != nil {
		slog.Error("Unable to ping database", slog.String("error", err.Error()))
		return nil, err
	}
	slog.Info("Successfully connected to database")

	return db, nil
}

func NewBasicPg(ctx context.Context, dbConfig *DBConfig) (*pgxpool.Pool, error) {
	// Connection URL
	connString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s",
		dbConfig.UserName, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.DBName)

	// Create a connection pool
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		slog.Error("Unable to create connection pool", slog.String("error", err.Error()))
		return nil, err
	}

	// Verify the connection
	if err = pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	slog.Info("Successfully connected to database")
	return pool, nil
}

func (app *App) DoExplicitConnectionOperations(ctx context.Context) error {
	// Acquire connection explicitly
	conn, err := app.DBClient.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("error acquiring connection: %v", err)
	}
	defer conn.Release()

	// Single record
	var userCount int
	err = conn.QueryRow(ctx,
		"SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		return fmt.Errorf("error counting users: %v", err)
	}
	slog.Info("User count", slog.Int("count", userCount))

	type User struct {
		Id   int
		Name string
	}
	// Multiple rows query
	rows, err := conn.Query(ctx,
		"SELECT id, name FROM users")
	if err != nil {
		return fmt.Errorf("error querying users: %v", err)
	}
	users, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[User])
	if err != nil {
		return fmt.Errorf("error reading rows: %v", err)
	}
	for _, user := range users {
		slog.Info("User retrieved", slog.Int("id", user.Id), slog.String("name", user.Name))
	}
	defer rows.Close()

	// Exec for insert/update/delete
	result, err := conn.Exec(ctx,
		"UPDATE users SET last_login = NOW() WHERE id = @id", pgx.NamedArgs{"id": 1})
	if err != nil {
		return fmt.Errorf("error updating user: %v", err)
	}
	slog.Info("Rows affected", slog.Int64("rows_affected", result.RowsAffected()))

	return nil
}

func (app *App) DoDirectPoolOperations(ctx context.Context) error {
	// Simple query directly using pool
	var userCount int
	err := app.DBClient.QueryRow(ctx,
		"SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		return fmt.Errorf("error executing query: %v", err)
	}
	slog.Info("User count", slog.Int("count", userCount))

	// Multiple rows query
	rows, err := app.DBClient.Query(ctx,
		"SELECT id, name FROM users")
	if err != nil {
		return fmt.Errorf("error querying users: %v", err)
	}
	for rows.Next() {
		var (
			id   int
			name string
		)
		err = rows.Scan(&id, &name)
		if err != nil {
			return fmt.Errorf("error reading user: %v", err)
		}
		slog.Info("User retrieved", slog.Int("id", id), slog.String("name", name))
	}
	defer rows.Close()

	// Exec for insert/update/delete
	result, err := app.DBClient.Exec(ctx,
		"UPDATE users SET last_login = NOW() WHERE id = @id", pgx.NamedArgs{"id": 1})
	if err != nil {
		return fmt.Errorf("error updating user: %v", err)
	}
	slog.Info("Rows affected", slog.Int64("rows_affected", result.RowsAffected()))

	return nil
}

func (app *App) monitorPoolStats() {
	stats := app.DBClient.Stat()

	slog.Info("Pool stats",
		slog.Int("total_connections", int(stats.TotalConns())),
		slog.Int("acquired_connections", int(stats.AcquiredConns())),
		slog.Int("idle_connections", int(stats.IdleConns())),
		slog.Int("max_connections", int(stats.MaxConns())),
	)
}
