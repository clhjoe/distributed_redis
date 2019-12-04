package redis

func (c *Ring) SAdd(key string, members ...interface{}) (reply interface{}, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.SAdd(key, members...).Result()
	if err != nil {
		return nil, err
	}
	return
}

func (c *Ring) SPop(key string, count int64) (reply []string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.SPopN(key, count).Result()
	if err != nil {
		return nil, err
	}
	return
}

func (c *Ring) SRandMember(key string, count int64) (reply []string, err error) {
	shard, rerr := c.getShardByKey(key)

	if rerr != nil {
		return nil, rerr
	}
	reply, err = shard.SRandMemberN(key, count).Result()
	if err != nil {
		return nil, err
	}
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
