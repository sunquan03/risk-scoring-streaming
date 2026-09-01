package configs

import (
	"os"

	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/package/utils"
)

type Config struct {
	TiDBHost     string
	TiDBPort     int
	TiDBUser     string
	TiDBPassword string
	TiDBDatabase string
	TiDBMaxConns int
	TiDBMaxIdle  int
	APIAddr      string
}

func LoadConfig() *Config {
	return &Config{
		TiDBHost:     utils.GetEnv("TIDB_HOST", "127.0.0.1"),
		TiDBPort:     utils.GetEnvInt("TiDB_PORT", 3306),
		TiDBUser:     utils.GetEnv("TIDB_USER", "risks"),
		TiDBPassword: utils.GetEnv("TIDB_PASSWORD", ""),
		TiDBDatabase: utils.GetEnv("TIDB_DATABASE", "risks"),
		TiDBMaxConns: utils.GetEnvInt("TIDB_MAX_CONNS", 20),
		TiDBMaxIdle:  utils.GetEnvInt("TIDB_MAX_IDLE", 300),
		APIAddr:      os.Getenv("API_ADDR"),
	}
}
