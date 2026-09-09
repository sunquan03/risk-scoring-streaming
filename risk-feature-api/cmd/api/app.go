package main

import (
	"log"

	"github.com/gin-gonic/gin"

	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/configs"
	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/handlers"
	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/tidb"
)

func main() {
	cfg := configs.LoadConfig()
	tidb.Init(*cfg)

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	v1 := r.Group("/v1")
	{
		v1.GET("/health", func(c *gin.Context) {
			c.JSON(200, gin.H{"status": "OK"})
		})

		v1.GET("/risk-profile/:client_id", handlers.GetRiskProfile)
		v1.GET("/risk-profile/:client_id/:group_id", handlers.GetFeatureGroup)
		v1.DELETE("/risk-profile/:client_id/cache", handlers.InvalidateCache)
	}

	log.Printf("risk-feature-api starting on %s", cfg.APIAddr)

	if err := r.Run(cfg.APIAddr); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
