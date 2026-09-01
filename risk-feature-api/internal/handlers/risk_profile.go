package handlers

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/features"
	"github.com/sunquan03/risk-scoring-streaming/risk-feature-api/internal/tidb"
)

type RiskProfileResponse struct {
	ClientID      string                            `json:"client_id"`
	FetchedAt     time.Time                         `json:"fetched_at"`
	CacheHit      bool                              `json:"cache_hit"`
	FeatureGroups map[string]map[string]interface{} `json:"feature_groups"`
	Errors        map[string]string                 `json:"errors,omitempty"`
}

func GetRiskProfile(c *gin.Context) {

	clientID := c.Param("client_id")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "client_id is required"})
		return
	}

	ctx := c.Request.Context()
	client := tidb.Get()
	db := client.DB()

	var results sync.Map
	var errors sync.Map
	var wg sync.WaitGroup
	for _, fg := range features.Groups {
		wg.Add(1)
		go func(group features.FeatureGroup) {
			defer wg.Done()

			cacheKey := group.CacheKey(clientID)

			if hit, ok := client.GetFromCache(ctx, cacheKey); ok {
				results.Store(group.ID, hit.Value)
				return
			}

			value, err := features.Compute(ctx, db, group.ID, clientID)
			if err != nil {
				errors.Store(group.ID, err.Error())
				return
			}

			ttl := time.Duration(group.TTL) * time.Second
			client.SetCache(ctx, cacheKey, value, &ttl)

			results.Store(group.ID, value)
		}(fg)
	}

	wg.Wait()

	featureGroups := make(map[string]map[string]interface{})
	results.Range(func(key, value any) bool {
		featureGroups[key.(string)] = value.(map[string]interface{})
		return true
	})
	errMap := make(map[string]string)
	errors.Range(func(key, value any) bool {
		errMap[key.(string)] = value.(string)
		return true
	})

	resp := RiskProfileResponse{
		ClientID:      clientID,
		FetchedAt:     time.Now().UTC(),
		CacheHit:      len(errMap) == 0 && len(featureGroups) == len(features.Groups),
		FeatureGroups: featureGroups,
	}
	if len(errMap) > 0 {
		resp.Errors = errMap
	}

	status := http.StatusOK
	if len(featureGroups) == 0 {
		status = http.StatusNotFound
	}

	c.JSON(status, resp)
}

func GetFeatureGroup(c *gin.Context) {
	clientID := c.Param("client_id")
	groupID := c.Param("group_id")

	var target *features.FeatureGroup
	for _, fg := range features.Groups {
		if fg.ID == groupID {
			g := fg
			target = &g
			break
		}
	}
	if target == nil {
		ids := make([]string, len(features.Groups))
		for i, fg := range features.Groups {
			ids[i] = fg.ID
		}

		c.JSON(http.StatusNotFound, gin.H{
			"error":     "unknown feature group",
			"group_id":  groupID,
			"available": ids,
		})
		return
	}

	ctx := c.Request.Context()
	client := tidb.Get()
	cacheKey := target.CacheKey(clientID)

	if hit, ok := client.GetFromCache(ctx, cacheKey); ok {
		c.JSON(http.StatusOK, gin.H{
			"client_id":  clientID,
			"group_id":   groupID,
			"cache_hit":  true,
			"fetched_at": time.Now().UTC(),
			"features":   hit.Value,
		})
		return
	}

	value, err := features.Compute(ctx, client.DB(), groupID, clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ttl := time.Duration(target.TTL) * time.Second
	client.SetCache(ctx, cacheKey, value, &ttl)

	c.JSON(http.StatusOK, gin.H{
		"client_id":  clientID,
		"group_id":   groupID,
		"cache_hit":  false,
		"fetched_at": time.Now().UTC(),
		"features":   value,
	})
}

func InvalidateCache(c *gin.Context) {
	clientID := c.Param("client_id")
	ctx := c.Request.Context()
	db := tidb.Get().DB()

	result, err := db.ExecContext(ctx, "delete from kv_cache where cache_key like ?", fmt.Sprintf("client:%s:%%", clientID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	affected, _ := result.RowsAffected()

	c.JSON(http.StatusOK, gin.H{
		"client_id":    clientID,
		"keys_deleted": affected,
	})
}
