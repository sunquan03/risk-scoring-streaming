package tidb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
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

func tlsParam(mode string) (string, error) {
	switch v := strings.TrimSpace(mode); v {
	case "", "false":
		return "", nil
	case "true", "skip-verify", "preferred":
		return "&tls=" + v, nil
	default:
		pem, err := os.ReadFile(v)
		if err != nil {
			return "", fmt.Errorf("read CA bundle %q: %w", v, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return "", errors.New("no certificates parsed from CA bundle")
		}
		if err := mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: pool}); err != nil {
			return "", fmt.Errorf("register TLS config: %w", err)
		}
		return "&tls=custom", nil
	}
}

func Init(cfg configs.Config) {
	once.Do(func() {
		tlsFrag, err := tlsParam(cfg.TiDBTLS)
		if err != nil {
			panic(fmt.Sprintf("tidb: TLS config: %v", err))
		}

		dsn := fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s"+
				"?charset=utf8mb4&parseTime=True&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s%s",
			cfg.TiDBUser, cfg.TiDBPassword, cfg.TiDBHost, cfg.TiDBPort, cfg.TiDBDatabase, tlsFrag,
		)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(fmt.Sprintf("tidb: sql.Open failed: %v", err))
		}
		maxIdle := cfg.TiDBMaxIdle
		if cfg.TiDBMaxConns > 0 && maxIdle > cfg.TiDBMaxConns {
			maxIdle = cfg.TiDBMaxConns
		}
		db.SetMaxOpenConns(cfg.TiDBMaxConns)
		db.SetMaxIdleConns(maxIdle)
		db.SetConnMaxLifetime(4 * time.Minute)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			panic(fmt.Sprintf("tidb: ping %s:%d/%s failed: %v",
				cfg.TiDBHost, cfg.TiDBPort, cfg.TiDBDatabase, err))
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
