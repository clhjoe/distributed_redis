package redis

import (
	"fmt"
	"time"
)

//------------------------------------------------------------------------------

func (c *Ring) Exists(key string) (int64, error) {
	shard, rerr := c.getShardByKey(key)
	if rerr != nil {
		return 0, rerr
	}
	return shard.Exists(key).Result()
}

func (c *Ring) Del(keys ...string) (reply interface{}, err error) {
	//	shard, rerr := c.getShardByKey(keys[0])

	//	if rerr != nil {
	//		return nil, rerr
	//	}
	//	rerr = shard.Del(keys...).Err()
	//	if rerr != nil {
	//		return nil, rerr
	//	}

	//	return
	task := make([][]string, c.ShardSize)
	for _, key := range keys {
		shardNum, _ := c.getShardNumberByKey(key)
		task[shardNum] = append(task[shardNum], key)
	}
	for shardNum, v := range task {
		if len(v) == 0 {
			continue
		}
		shard, _ := c.getShardByRedisNumber(shardNum)
		errors := shard.Del(keys...).Err()
		if errors != nil {
			err = errors
			fmt.Println(errors.Error())
			continue
		}

	}

	return reply, err
}
func (c *Ring) MultiDel(keys ...string) (reply interface{}, err error) {
	task := make([][]string, c.ShardSize)
	shardErr := make(chan error, c.ShardSize)
	for _, key := range keys {
		shardNum, _ := c.getShardNumberByKey(key)
		task[shardNum] = append(task[shardNum], key)
	}
	for shardNum, v := range task {
		if len(v) == 0 {
			continue
		}

		go func(shardNum int, keys ...string) {
			shard, _ := c.getShardByRedisNumber(shardNum)
			err := shard.Del(keys...).Err()
			shardErr <- err
		}(shardNum, v...)

	}

	close(shardErr)
	for r := range shardErr {
		if r != nil {
			err = r
			//			fmt.Println(err.Error())
		}
	}
	return
}
func (c *Ring) Unlink(keys ...string) (reply interface{}, err error) {
	for _, key := range keys {
		shard, rerr := c.getShardByKey(key)

		if rerr != nil {
			return nil, rerr
		}
		rerr = shard.Unlink(key).Err()
		if rerr != nil {
			return nil, rerr
		}
	}
	return
}

//func (c *Ring) Dump(key string) (reply interface{}, err error) {
//	err = errNotImplemented
//	return
//}

//func (c *Ring) Exists(keys ...string) (reply interface{}, err error) {
//	err = errNotImplemented
//	return
//}

func (c *Ring) Expire(key string, expiration time.Duration) (err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return rerr
	}
	rerr = shard.Expire(key, expiration).Err()
	if rerr != nil {
		return rerr
	}

	return
}

func (c *Ring) ExpireAt(key string, tm time.Time) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	rerr = shard.ExpireAt(key, tm).Err()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) TTL(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.TTL(key).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) Decr(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.Decr(key).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) DecrBy(key string, decrement int64) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.DecrBy(key, decrement).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) Get(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.Get(key).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) GetBit(key string, offset int64) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.GetBit(key, offset).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) GetRange(key string, start, end int64) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.GetRange(key, start, end).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) GetSet(key string, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.GetSet(key, value).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) Incr(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.Incr(key).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) IncrBy(key string, value int64) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.IncrBy(key, value).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) IncrByFloat(key string, value float64) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, rerr = shard.IncrByFloat(key, value).Result()
	if rerr != nil {
		return nil, rerr
	}

	return
}

func (c *Ring) MGet(keys ...string) (reply []interface{}, err error) {
	//	shard, rerr := c.getShardByKey(keys[0])
	//	val, rerr := shard.MGet(keys...).Result()
	//	if rerr != nil {
	//		return nil, rerr
	//	}

	//	return val, nil
	reverseKeys := make(map[int][]int, c.ShardSize)
	reply = make([]interface{}, len(keys))
	task, reverseKeys, err := c.splitKeystoShards(keys...)
	if err != nil {
		return
	}

	for shardNum, v := range task {
		if len(v) == 0 {
			continue
		}
		shard, _ := c.getShardByRedisNumber(shardNum)
		res, errors := shard.MGet(v...).Result()
		if errors != nil {
			err = errors
			fmt.Println(err.Error())
			continue
		}
		for keyIndex, val := range res {
			reply[reverseKeys[shardNum][keyIndex]] = val
		}
	}

	return reply, err
}

func (c *Ring) MultiMGet(keys ...string) (reply []interface{}, err error) {
	taskRes := make([][]interface{}, c.ShardSize)
	shardErr := make(chan error, c.ShardSize)
	reply = make([]interface{}, len(keys))
	task, reverseKeys, err := c.splitKeystoShards(keys...)
	if err != nil {
		return
	}

	for shardNum, v := range task {
		if len(v) == 0 {
			continue
		}
		go func(shardNum int, subkeys ...string) {
			shard, _ := c.getShardByRedisNumber(shardNum)
			taskRes[shardNum], err = shard.MGet(subkeys...).Result()
			shardErr <- err
		}(shardNum, v...)
	}
	close(shardErr)
	for r := range shardErr {
		err = r
		fmt.Println(r.Error())
	}

	for shardIndex, vals := range taskRes {
		for keyIndex, val := range vals {
			reply[reverseKeys[shardIndex][keyIndex]] = val
		}
	}

	return reply, err
}
func (c *Ring) MSet(pairs ...interface{}) (reply interface{}, err error) {
	task := make([][]interface{}, c.ShardSize)
	arrlen := len(pairs)
	for i := 0; i < arrlen; i += 2 {
		k := (pairs[i]).(string)
		v := (pairs[i+1]).(string)
		shardNum, _ := c.getShardNumberByKey(k)
		task[shardNum] = append(task[shardNum], k, v)
	}
	for shardNum, v := range task {
		if len(v) == 0 {
			continue
		}
		shard, _ := c.getShardByRedisNumber(shardNum)
		errors := shard.MSet(v...).Err()
		if errors != nil {
			err = errors
			fmt.Println(err.Error())
		}
	}

	return reply, err
}

func (c *Ring) MSetNX(pairs ...interface{}) (reply interface{}, err error) {
	panic("not implemented")
}

// Redis `SET key value [expiration]` command.
//
// Use expiration for `SETEX`-like behavior.
// Zero expiration means the key has no expiration time.
func (c *Ring) Set(key string, value interface{}, expiration time.Duration) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)
	rerr = shard.Set(key, value, expiration).Err()
	if rerr != nil {
		return nil, rerr
	}
	return
}

//func (c *Ring) SetBit(key string, offset int64, value int) (reply interface{}, err error) {

//}

// Redis `SET key value [expiration] NX` command.
//
// Zero expiration means the key has no expiration time.
func (c *Ring) SetNX(key string, value interface{}, expiration time.Duration) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.SetNX(key, value, expiration).Result()
	return
}

// Redis `SET key value [expiration] XX` command.
// Zero expiration means the key has no expiration time.
func (c *Ring) SetXX(key string, value interface{}, expiration time.Duration) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.SetXX(key, value, expiration).Result()
	return
}

func (c *Ring) Eval(script string, key string, args ...interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.Eval(script, []string{key}, args...).Result()
	return
}

/*
func (c *Ring) SetRange(key string, offset int64, value string) (reply interface{}, err error) {
	cmd := r.NewIntCmd("setrange", key, offset, value)
	c.process(cmd)
	return cmd
}

func (c *Ring) StrLen(key string) (reply interface{}, err error) {
	cmd := r.NewIntCmd("strlen", key)
	c.process(cmd)
	return cmd
}
*/
//------------------------------------------------------------------------------
