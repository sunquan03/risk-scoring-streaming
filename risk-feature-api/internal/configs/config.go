package configs

import (
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
		TiDBPort:     utils.GetEnvInt("TIDB_PORT", 4000),
		TiDBUser:     utils.GetEnv("TIDB_USER", "root"),
		TiDBPassword: utils.GetEnv("TIDB_PASSWORD", ""),
		TiDBDatabase: utils.GetEnv("TIDB_DATABASE", "risk_pipeline"),
		TiDBMaxConns: utils.GetEnvInt("TIDB_MAX_CONNS", 20),
		TiDBMaxIdle:  utils.GetEnvInt("TIDB_MAX_IDLE", 10),
		APIAddr:      utils.GetEnv("API_ADDR", ":8080"),
	}
}
