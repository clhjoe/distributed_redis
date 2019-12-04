package redis

import (
	"errors"
	"reflect"
)

const (
	structTag = "redis"
)

func (c *Ring) HIncrBy(key, field string, incr int64) (reply int64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.HIncrBy(key, field, incr).Result()
	if err != nil {
		return 0, err
	}

	return
}

func (c *Ring) HIncrByFloat(key, field string, incr float64) (reply float64, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return 0, rerr
	}
	reply, err = shard.HIncrByFloat(key, field, incr).Result()
	if err != nil {
		return 0, err
	}

	return
}

//func (c *Ring) HKeys(key string) (reply interface{}, err error) {
//	shard, rerr := c.getShardByKey(key)

//	if rerr != nil {
//		return nil, rerr
//	}
//	reply, err = shard.HIncrByFloat(key, field, incr).Result()
//	if err != nil {
//		return nil, rerr
//	}

//	return
//}

func (c *Ring) HLen(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.HLen(key).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) HMGet(key string, fields ...string) (reply []interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.HMGet(key, fields...).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) HMSet(key string, fields map[string]interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.HMSet(key, fields).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) HMSetStruct(key string, obj interface{}) (reply interface{}, err error) {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	var data = make(map[string]interface{})

	for i := 0; i < t.NumField(); i++ {
		redisTag := t.Field(i).Tag.Get(structTag)
		if redisTag == "-" || redisTag == "" {
			continue
		}
		data[redisTag] = v.Field(i).Interface()
	}
	if len(data) == 0 {
		return
	}
	return c.HMSet(key, data)
}

func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		if encountered[elements[v]] != true {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}

//PipelineMultiHGetAll allows you to retrieve multiple hash data from different shards by pipeline and goroutine
func (c *Ring) PipelineMultiHGetAll(inputKeys ...string) (reply map[string]map[string]string, err error) {
	existsKeys := map[string]bool{}
	task := make([][]string, c.ShardSize)
	shardErr := make(chan error, c.ShardSize)
	shardRes := make(chan map[string]map[string]string, c.ShardSize)

	//將key放到不同的shard陣列中，並且忽略重複的key
	for _, key := range inputKeys {
		if _, exist := existsKeys[key]; exist {
			continue
		}
		existsKeys[key] = true
		shardNum, _ := c.getShardNumberByKey(key)
		task[shardNum] = append(task[shardNum], key)
	}
	reply = make(map[string]map[string]string, len(existsKeys))
	//每個shard陣列用一個goroutine利用pipeline取得資料
	for shardNum, v := range task {
		if len(v) == 0 {
			shardRes <- nil
			shardErr <- nil
			continue
		}

		go func(shardNum int, keys ...string) {
			shard, _ := c.getShardByRedisNumber(shardNum)
			cmds := make(map[string]StringStringMapCmd, len(keys))
			pl := shard.Pipeline()
			for _, k := range keys {
				cmds[k] = pl.HGetAll(k)
			}
			pl.Exec()
			res := make(map[string]map[string]string, len(keys))
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

//MultiHGetAll allows you to retrieve multiple hash data from different shards by goroutine
func (c *Ring) MultiHGetAll(keys ...string) (reply map[string]map[string]string, err error) {
	keys = removeDuplicates(keys)
	//use channel to make sure return after all goroutines finished their job
	length := len(keys)
	reply = make(map[string]map[string]string)

	shardErr := make(chan error, length)
	shardRes := make(chan map[string]interface{}, length)
	for _, key := range keys {
		go func(key string) {
			res := make(map[string]interface{})
			r, errs := c.HGetAll(key)
			res["key"] = key
			res["content"] = r
			shardErr <- errs
			shardRes <- res

		}(key)
	}
	errMsg := ""
	for i := 0; i < length; i++ {
		if r := <-shardErr; r != nil {
			errMsg += "," + r.Error()
		}
		res := <-shardRes
		if len(res) > 0 {
			reply[res["key"].(string)] = res["content"].(map[string]string)
		}
	}
	if errMsg != "" {
		err = errors.New(errMsg)
	}
	return
}

func (c *Ring) HSet(key, field string, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.HSet(key, field, value).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) HSetNX(key, field string, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.HSetNX(key, field, value).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) HVals(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.HVals(key).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) HDel(key string, fields ...string) (err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return rerr
	}
	rerr = shard.HDel(key, fields...).Err()
	if rerr != nil {
		return rerr
	}

	return
}

func (c *Ring) HExists(key, field string) (reply bool, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return false, rerr
	}
	reply, err = shard.HExists(key, field).Result()
	if err != nil {
		return false, err
	}

	return
}

func (c *Ring) HGet(key, field string) (reply string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return "", rerr
	}
	reply, err = shard.HGet(key, field).Result()
	if err != nil {
		return "", err
	}

	return
}

func (c *Ring) HGetAll(key string) (reply map[string]string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}

	return
}
