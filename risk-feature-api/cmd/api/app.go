package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/tidb"

	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/configs"
	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/handlers"
	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/package/utils"
)

func main() {
	tidb.Init(*configs.LoadConfig())
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

	}

	addr := utils.GetEnv("APP_HOST", ":8080")
	log.Printf("risk-feature-api starting on %s", addr)

	if err := r.Run(addr); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
