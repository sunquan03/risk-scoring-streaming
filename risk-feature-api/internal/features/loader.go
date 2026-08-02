package features

import "fmt"

type FeatureGroup struct {
	ID               string
	CacheKeyTemplate string
	TTL              int
}

var Groups = []FeatureGroup{
	{
		ID:               "loan_app_features",
		CacheKeyTemplate: "client:%s:loan_apps",
		TTL:              1800,
	},
	{
		ID:               "payment_features",
		CacheKeyTemplate: "client:%s:payments",
		TTL:              3600,
	},
	{
		ID:               "operation_features",
		CacheKeyTemplate: "client:%s:operations",
		TTL:              3600,
	},
	{
		ID:               "money_features",
		CacheKeyTemplate: "client:%s:money",
		TTL:              7200,
	},
}

func (fg FeatureGroup) CacheKey(clientID string) string {
	return fmt.Sprintf(fg.CacheKeyTemplate, clientID)
}
