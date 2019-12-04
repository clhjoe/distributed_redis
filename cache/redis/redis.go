package redis

import (
	"github.com/go-redis/redis"
)

type Z = redis.Z
type Client = redis.Client
type FailoverOptions = redis.FailoverOptions
type Options = redis.Options
type StringStringMapCmd = *redis.StringStringMapCmd

func NewFailoverClient(failoverOpt *FailoverOptions) *Client {
	return redis.NewFailoverClient(failoverOpt)
}

func NewClient(opt *Options) *Client {
	return redis.NewClient(opt)
}
