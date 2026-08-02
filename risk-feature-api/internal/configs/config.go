package configs

import "os"

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
		TiDBHost:     getEnv("TIDB_HOST", "127.0.0.1"),
		TiDBPort:     getEnvInt("TiDB_PORT", 3306),
		TiDBUser:     getEnv("TIDB_USER", "risks"),
		TiDBPassword: getEnv("TIDB_PASSWORD", ""),
		TiDBDatabase: getEnv("TIDB_DATABASE", "risks"),
		TiDBMaxConns: getEnvInt("TIDB_MAX_CONNS", 20),
		TiDBMaxIdle:  getEnvInt("TIDB_MAX_IDLE", 300),
		APIAddr:      os.Getenv("API_ADDR"),
	}
}
