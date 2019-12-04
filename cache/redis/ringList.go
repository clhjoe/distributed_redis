package redis

import (
	"time"
)

//------------------------------------------------------------------------------
func (c *Ring) BLPop(timeout time.Duration, keys ...string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(keys[0])

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.BLPop(timeout, keys...).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) BRPop(timeout time.Duration, keys ...string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(keys[0])

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.BRPop(timeout, keys...).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) BRPopLPush(source, destination string, timeout time.Duration) (reply interface{}, err error) {
	return nil, errNotImplemented
	//	shard, rerr := c.getShardByKey(key)

	//	if rerr != nil {
	//		return nil, rerr
	//	}
	//	reply, err = shard.BRPopLPush(source, destination, timeout).Result()
	//	if err != nil {
	//		return nil, rerr
	//	}

	//	return
}

func (c *Ring) LIndex(key string, index int64) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LIndex(key, index).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LInsert(key, op string, pivot, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LInsert(key, op, pivot, value).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LInsertBefore(key string, pivot, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LInsertBefore(key, pivot, value).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LInsertAfter(key string, pivot, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LInsertAfter(key, pivot, value).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LLen(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LLen(key).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LPop(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LPop(key).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LPush(key string, values ...interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LPush(key, values...).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LPushX(key string, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LPushX(key, value).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LRange(key string, start, stop int64) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LRange(key, start, stop).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LRem(key string, count int64, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LRem(key, count, value).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LSet(key string, index int64, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LSet(key, index, value).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) LTrim(key string, start, stop int64) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.LTrim(key, start, stop).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) RPop(key string) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.RPop(key).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) RPopLPush(source, destination string) (reply interface{}, err error) {
	return nil, errNotImplemented
	//	shard, rerr := c.getShardByKey(key)

	//	if rerr != nil {
	//		return nil, rerr
	//	}
	//	reply, err = shard.RPopLPush(source, destination).Result()
	//	if err != nil {
	//		return nil, rerr
	//	}

	//	return
}

func (c *Ring) RPush(key string, values ...interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.RPush(key, values...).Result()
	if err != nil {
		return nil, err
	}

	return
}

func (c *Ring) RPushX(key string, value interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.RPushX(key, value).Result()
	if err != nil {
		return nil, err
	}

	return
}
