package features

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func Compute(ctx context.Context, db *sql.DB, groupID string, clientID string) (map[string]any, error) {
	query, ok := aggregationSQL[groupID]
	if !ok {
		return nil, fmt.Errorf("unknown group %s", groupID)
	}

	params := make([]any, strings.Count(query, "?"))
	for i := range params {
		params[i] = clientID
	}

	var raw []byte
	err := db.QueryRowContext(ctx, query, params...).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("group %s: %w", groupID, err)
	}

	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("group %s: decoding features: %w", groupID, err)
	}
	return result, nil
}

func GroupIDs() []string {
	ids := make([]string, 0, len(aggregationSQL))
	for id := range aggregationSQL {
		ids = append(ids, id)
	}
	return ids
}
