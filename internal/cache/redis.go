package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/Shadi/trino-log-sink/internal/config"
	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(cfg config.Cache) *Redis {
	opts := &redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     10,
		DialTimeout:  cfg.Timeout,
		ReadTimeout:  cfg.Timeout,
		WriteTimeout: cfg.Timeout,
		PoolTimeout:  cfg.Timeout,
		MaxRetries:   -1,
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return &Redis{client: redis.NewClient(opts)}
}

func (r *Redis) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := r.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrMiss
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (r *Redis) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, val, ttl).Err()
}

func (r *Redis) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *Redis) Close() error {
	return r.client.Close()
}
