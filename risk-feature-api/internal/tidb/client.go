package tidb

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/configs"
)

type Client struct {
	db *sql.DB
}

var (
	instance *Client
	once     sync.Once
)

func Init(cfg configs.Config) {
	once.Do(func() {
		dsn := fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=UTC",
			cfg.TiDBUser, cfg.TiDBPassword, cfg.TiDBHost, cfg.TiDBPort, cfg.TiDBDatabase,
		)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(fmt.Sprintf("tidb: sql.Open failed: %v", err))
		}
		db.SetMaxOpenConns(cfg.TiDBMaxConns)
		db.SetMaxIdleConns(cfg.TiDBMaxIdle)
		db.SetConnMaxLifetime(4 * time.Minute)

		if err := db.Ping(); err != nil {
			panic(fmt.Sprintf("tidb: ping failed: %v", err))
		}
		instance = &Client{db: db}
	})
}

func Get() *Client {
	if instance == nil {
		panic("tidb: Get called before Init")
	}
	return instance
}

func (c *Client) DB() *sql.DB {
	return c.db
}
