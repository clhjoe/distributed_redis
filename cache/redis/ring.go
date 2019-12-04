package redis

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/go-redis/redis"
)

var (
	errRingShardsDown = errors.New("redis: all ring shards are down")
	errNotImplemented = errors.New("redis: method not implemented")
)

const Nil = RedisError("redis: nil")

type RedisError string

// RingOptions are used to configure a ring client and should be
// passed to NewRing.
type RingOptions *redis.FailoverOptions

//type ringShard *redis.Client

type Ring struct {
	mu        sync.RWMutex
	shards    map[int]*redis.Client
	ShardSize int
	Prefix    string
}

func NewRing(rcs map[int]*redis.Client, prefix string) *Ring {
	ring := &Ring{
		shards:    rcs,
		ShardSize: len(rcs),
		Prefix:    prefix,
	}
	for i, r := range rcs {
		pong, err := r.Ping().Result()
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf(pong+" from ring: %d \n", i)
	}
	return ring
}

func (c *Ring) addClient(number int, rc *redis.Client) {
	c.mu.Lock()
	fmt.Println(rc.Ping())
	c.shards[number] = rc
	c.ShardSize++
	c.mu.Unlock()
}

func (c *Ring) getShardByKey(key string) (*redis.Client, error) {
	//	key = r.hashtag.Key(key)

	c.mu.RLock()
	key = strings.TrimPrefix(key, c.Prefix)
	hasher := md5.New()
	hasher.Write([]byte(key))

	shard := c.shards[int(hex.EncodeToString(hasher.Sum(nil))[0])%c.ShardSize]

	c.mu.RUnlock()
	return shard, nil
}

func (c *Ring) getShardNumberByKey(key string) (int, error) {
	key = strings.TrimPrefix(key, c.Prefix)
	hasher := md5.New()
	hasher.Write([]byte(key))

	return int(hex.EncodeToString(hasher.Sum(nil))[0]) % c.ShardSize, nil
}

func (c *Ring) getShardByRedisNumber(number int) (*redis.Client, error) {
	if number >= c.ShardSize {
		return nil, errors.New("redis: shard number exceeds number of shards")
	}

	c.mu.RLock()
	shard := c.shards[number]
	c.mu.RUnlock()
	return shard, nil
}

func (c *Ring) splitKeystoShards(keys ...string) (task [][]string, reverseKeys map[int][]int, err error) {
	//[#shard][index of the keys]
	reverseKeys = make(map[int][]int, c.ShardSize)

	//[#shard][keys]
	task = make([][]string, c.ShardSize)
	for index, key := range keys {
		shardNum, errors := c.getShardNumberByKey(key)
		if errors != nil {
			return nil, nil, errors
		}
		task[shardNum] = append(task[shardNum], key)
		reverseKeys[shardNum] = append(reverseKeys[shardNum], index)
	}
	return
}

// Publish posts the message to the channel.
func (c *Ring) Publish(channel, message string) (reply interface{}, err error) {
	return nil, errNotImplemented
}

func (c *Ring) PubSubChannels(pattern string) (reply interface{}, err error) {
	return nil, errNotImplemented
}

func (c *Ring) PubSubNumSub(channels ...string) (reply interface{}, err error) {
	return nil, errNotImplemented
}

func (c *Ring) PubSubNumPat() (reply interface{}, err error) {
	return nil, errNotImplemented
}

func (c *Ring) Command() (reply interface{}, err error) {
	//	cmd := r.NewCommandsInfoCmd("command")
	//	c.process(cmd)
	//	return cmd
	return
}
