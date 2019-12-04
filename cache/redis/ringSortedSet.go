package redis

import (
	"errors"

	"github.com/go-redis/redis"
)

// Redis `ZADD key score member [score member ...]` command.
func (c *Ring) ZAdd(key string, members ...Z) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZAdd(key, members...).Result()
	if err != nil {
		return 0, err
	}

	return
}

// Redis `ZADD key NX score member [score member ...]` command.
func (c *Ring) ZAddNX(key string, members ...redis.Z) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZAddNX(key, members...).Result()
	if err != nil {
		return 0, err
	}

	return
}

// Redis `ZADD key XX score member [score member ...]` command.
func (c *Ring) ZAddXX(key string, members ...Z) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZAddXX(key, members...).Result()
	if err != nil {
		return 0, err
	}

	return
}

// Redis `ZADD key CH score member [score member ...]` command.
func (c *Ring) ZAddCh(key string, members ...Z) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZAddCh(key, members...).Result()
	if err != nil {
		return 0, err
	}

	return
}

// Redis `ZADD key NX CH score member [score member ...]` command.
func (c *Ring) ZAddNXCh(key string, members ...Z) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZAddNXCh(key, members...).Result()
	if err != nil {
		return 0, err
	}

	return
}

// Redis `ZADD key XX CH score member [score member ...]` command.
func (c *Ring) ZAddXXCh(key string, members ...Z) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZAddXXCh(key, members...).Result()
	if err != nil {
		return 0, err
	}

	return
}

// Redis `ZADD key INCR score member` command.
func (c *Ring) ZIncr(key string, member Z) (reply float64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZIncr(key, member).Result()
	if err != nil {
		return 0, err
	}

	return
}

// Redis `ZADD key NX INCR score member` command.
func (c *Ring) ZIncrNX(key string, member Z) (reply float64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZIncrNX(key, member).Result()
	if err != nil {
		return 0, err
	}

	return
}

// Redis `ZADD key XX INCR score member` command.
func (c *Ring) ZIncrXX(key string, member Z) (reply float64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZIncrXX(key, member).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZCard(key string) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZCard(key).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZCount(key, min, max string) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZCount(key, min, max).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZIncrBy(key string, increment float64, member string) (reply float64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZIncrBy(key, increment, member).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZInterStore(destination string, store redis.ZStore, keys ...string) (reply int64, err error) {
	return 0, errNotImplemented
	shard, rerr := c.getShardByKey(keys[0])

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZInterStore(destination, store, keys...).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZRange(key string, start, stop int64) (reply []string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRange(key, start, stop).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRangeWithScores(key string, start, stop int64) (reply []Z, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRangeWithScores(key, start, stop).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRangeByScore(key string, opt redis.ZRangeBy) (reply []string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRangeByScore(key, opt).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRangeByLex(key string, opt redis.ZRangeBy) (reply []string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRangeByLex(key, opt).Result()
	if err != nil {
		return nil, err
	}

	return
}
func (c *Ring) PipelineMultiZRevRangeByScoreWithScores(keys []string, opt redis.ZRangeBy) (reply map[string][]Z, err error) {
	dedupKeys := map[string]bool{}
	task := make([][]string, c.ShardSize)
	shardErr := make(chan error, c.ShardSize)
	shardRes := make(chan map[string][]Z, c.ShardSize)

	//將key放到不同的shard陣列中，並且忽略重複的key
	for _, key := range keys {
		if _, exist := dedupKeys[key]; exist {
			continue
		}
		dedupKeys[key] = true
		shardNum, _ := c.getShardNumberByKey(key)
		task[shardNum] = append(task[shardNum], key)
	}
	reply = make(map[string][]Z, len(dedupKeys))
	//每個shard陣列用一個goroutine利用pipeline取得資料
	for shardNum, v := range task {
		if len(v) == 0 {
			shardRes <- nil
			shardErr <- nil
			continue
		}

		go func(shardNum int, keys ...string) {
			shard, _ := c.getShardByRedisNumber(shardNum)
			cmds := make(map[string]*redis.ZSliceCmd, len(keys))
			pl := shard.Pipeline()
			for _, k := range keys {
				cmds[k] = pl.ZRevRangeByScoreWithScores(k, opt)
			}
			pl.Exec()
			res := make(map[string][]Z, len(keys))
			var errs error
			var err error

			for k, v := range cmds {
				res[k], err = v.Result()
				if err != nil {
					errs = err
					break
				}
			}
			shardErr <- errs
			shardRes <- res

		}(shardNum, v...)

	}
	//recieve all res,err from channel
	errMsg := ""
	for i := 0; i < c.ShardSize; i++ {
		if r := <-shardErr; r != nil {
			errMsg += "," + r.Error()
		}
		res := <-shardRes
		if len(res) == 0 {
			continue
		}
		for k, v := range res {
			reply[k] = v
		}
	}
	if errMsg != "" {
		err = errors.New(errMsg)
	}
	return

}
func (c *Ring) ZRangeByScoreWithScores(key string, opt redis.ZRangeBy) (reply []Z, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRangeByScoreWithScores(key, opt).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRank(key, member string) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZRank(key, member).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZRem(key string, members ...interface{}) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZRem(key, members...).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZRemRangeByRank(key string, start, stop int64) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZRemRangeByRank(key, start, stop).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZRemRangeByScore(key, min, max string) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZRemRangeByScore(key, min, max).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZRemRangeByLex(key, min, max string) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZRemRangeByLex(key, min, max).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZRevRange(key string, start, stop int64) (reply []string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRevRange(key, start, stop).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRevRangeWithScores(key string, start, stop int64) (reply []Z, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRevRangeWithScores(key, start, stop).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRevRangeByScore(key string, opt redis.ZRangeBy) (reply []string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRevRangeByScore(key, opt).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRevRangeByLex(key string, opt redis.ZRangeBy) (reply []string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRevRangeByLex(key, opt).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRevRangeByScoreWithScores(key string, opt redis.ZRangeBy) (reply []Z, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.ZRevRangeByScoreWithScores(key, opt).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) ZRevRank(key, member string) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZRevRank(key, member).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZScore(key, member string) (reply float64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZScore(key, member).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) ZUnionStore(dest string, store redis.ZStore, keys ...string) (reply int64, err error) {
	return 0, errNotImplemented
	shard, rerr := c.getShardByKey(keys[0])

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.ZUnionStore(dest, store, keys...).Result()
	if err != nil {
		return 0, err
	}

	return
}
