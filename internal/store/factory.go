package store

import (
	"fmt"

	"github.com/Shadi/trino-log-sink/internal/config"
)

func Open(backend string, trino config.Trino, clickhouse config.ClickHouse) (Backend, error) {
	switch backend {
	case config.BackendClickHouse:
		s, err := NewClickHouse(clickhouse)
		if err != nil {
			return nil, err
		}
		return s, nil
	case config.BackendTrino, "":
		s, err := NewTrino(trino)
		if err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, fmt.Errorf("unknown store backend %q", backend)
	}
}
