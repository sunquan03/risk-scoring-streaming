package tidb

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

type CacheResult struct {
	Key        string
	Value      map[string]any
	ComputedAt time.Time
	ExpiresAt  *time.Time
	Version    int
}

func (c *Client) GetFromCache(ctx context.Context, cacheKey string) (*CacheResult, bool) {
	const query = `
		select cache_key, value_json, computed_at, expires_at, version
		from kv_cache
		where cache_key = ?
    `
	var (
		key        string
		valueJSON  []byte
		computedAt time.Time
		expiresAt  sql.NullTime
		version    int
	)
	err := c.db.QueryRowContext(ctx, query, cacheKey).Scan(
		&key, &valueJSON, &computedAt, &expiresAt, &version,
	)
	if err == sql.ErrNoRows {
		return nil, false
	}
	if err != nil {
		//todo add logs or smth
		return nil, false
	}

	if expiresAt.Valid && time.Now().UTC().After(expiresAt.Time) {
		return nil, false
	}

	var value map[string]any
	if err := json.Unmarshal(valueJSON, &value); err != nil {
		//todo add logs or smth
		return nil, false
	}

	var exp *time.Time

	if expiresAt.Valid {
		t := expiresAt.Time
		exp = &t
	}

	return &CacheResult{
		Key:        key,
		Value:      value,
		ComputedAt: computedAt,
		ExpiresAt:  exp,
		Version:    version,
	}, true
}

func (c *Client) SetCache(
	ctx context.Context,
	cacheKey string,
	value map[string]interface{},
	ttl *time.Duration,
) error {
	valJSON, err := json.Marshal(value)
	if err != nil {
		return err
	}

	var expiresAt any
	if ttl != nil {
		expiresAt = time.Now().UTC().Add(*ttl)
	}

	const query = `
			insert into kv_cache
					(cache_key, value_json, computed_at, expires_at, version)
				values
					(?, ?, NOW(3), ?, 1)
				on duplicate key update
					value_json  = VALUES(value_json),
					computed_at = NOW(3),
					expires_at  = VALUES(expires_at),
					version     = version + 1
			`
	_, err = c.db.ExecContext(ctx, query, cacheKey, valJSON, expiresAt)
	return err
}
