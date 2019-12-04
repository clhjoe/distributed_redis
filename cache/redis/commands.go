package redis

import (
	"time"

	"github.com/go-redis/redis"
)

func readTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return 0
	}
	return timeout + time.Second
}

func usePrecise(dur time.Duration) bool {
	return dur < time.Second || dur%time.Second != 0
}

func formatMs(dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		//		internal.Logf(
		//			"specified duration is %s, but minimal supported value is %s",
		//			dur, time.Millisecond,
		//		)
	}
	return int64(dur / time.Millisecond)
}

func formatSec(dur time.Duration) int64 {
	if dur > 0 && dur < time.Second {
		//		internal.Logf(
		//			"specified duration is %s, but minimal supported value is %s",
		//			dur, time.Second,
		//		)
	}
	return int64(dur / time.Second)
}

type Cmdable interface {
	Del(keys ...string) (reply interface{}, err error)
	Unlink(keys ...string) (reply interface{}, err error)
	Expire(key string, expiration time.Duration) (reply interface{}, err error)
	ExpireAt(key string, tm time.Time) (reply interface{}, err error)
	TTL(key string) (reply interface{}, err error)

	Decr(key string) (reply interface{}, err error)
	DecrBy(key string, decrement int64) (reply interface{}, err error)
	Get(key string) (reply interface{}, err error)
	GetBit(key string, offset int64) (reply interface{}, err error)
	GetRange(key string, start, end int64) (reply interface{}, err error)
	GetSet(key string, value interface{}) (reply interface{}, err error)
	Incr(key string) (reply interface{}, err error)
	IncrBy(key string, value int64) (reply interface{}, err error)
	IncrByFloat(key string, value float64) (reply interface{}, err error)
	MGet(keys ...string) (reply interface{}, err error)
	MSet(pairs ...interface{}) (reply interface{}, err error)
	MSetNX(pairs ...interface{}) (reply interface{}, err error)
	Set(key string, value interface{}, expiration time.Duration) (reply interface{}, err error)
	SetBit(key string, offset int64, value int) (reply interface{}, err error)
	SetNX(key string, value interface{}, expiration time.Duration) (reply interface{}, err error)
	SetXX(key string, value interface{}, expiration time.Duration) (reply interface{}, err error)
	SetRange(key string, offset int64, value string) (reply interface{}, err error)
	StrLen(key string) (reply interface{}, err error)
	HDel(key string, fields ...string) (reply interface{}, err error)
	HExists(key, field string) (reply interface{}, err error)
	HGet(key, field string) (reply interface{}, err error)
	HGetAll(key string) *redis.StringStringMapCmd
	HIncrBy(key, field string, incr int64) (reply interface{}, err error)
	HIncrByFloat(key, field string, incr float64) (reply interface{}, err error)
	//HKeys(key string) (reply interface{}, err error)
	HLen(key string) (reply interface{}, err error)
	HMGet(key string, fields ...string) (reply interface{}, err error)
	HMSet(key string, fields map[string]string) (reply interface{}, err error)
	HSet(key, field string, value interface{}) (reply interface{}, err error)
	HSetNX(key, field string, value interface{}) (reply interface{}, err error)
	HVals(key string) (reply interface{}, err error)
	BLPop(keys ...string) (reply interface{}, err error)
	BRPop(keys ...string) (reply interface{}, err error)
	BRPopLPush(source, destination string, timeout time.Duration) (reply interface{}, err error)
	LIndex(key string, index int64) (reply interface{}, err error)
	LInsert(key, op string, pivot, value interface{}) (reply interface{}, err error)
	LInsertBefore(key string, pivot, value interface{}) (reply interface{}, err error)
	LInsertAfter(key string, pivot, value interface{}) (reply interface{}, err error)
	LLen(key string) (reply interface{}, err error)
	LPop(key string) (reply interface{}, err error)
	LPush(key string, values ...interface{}) (reply interface{}, err error)
	LPushX(key string, value interface{}) (reply interface{}, err error)
	LRange(key string, start, stop int64) (reply interface{}, err error)
	LRem(key string, count int64, value interface{}) (reply interface{}, err error)
	LSet(key string, index int64, value interface{}) (reply interface{}, err error)
	LTrim(key string, start, stop int64) (reply interface{}, err error)
	RPop(key string) (reply interface{}, err error)
	RPopLPush(source, destination string) (reply interface{}, err error)
	RPush(key string, values ...interface{}) (reply interface{}, err error)
	RPushX(key string, value interface{}) (reply interface{}, err error)
	SAdd(key string, members ...interface{}) (reply interface{}, err error)
	SCard(key string) (reply interface{}, err error)
	SDiff(keys ...string) (reply interface{}, err error)
	SDiffStore(destination string, keys ...string) (reply interface{}, err error)
	SInter(keys ...string) (reply interface{}, err error)
	SInterStore(destination string, keys ...string) (reply interface{}, err error)
	SIsMember(key string, member interface{}) (reply interface{}, err error)
	SMembers(key string) (reply interface{}, err error)
	SMove(source, destination string, member interface{}) (reply interface{}, err error)
	SPop(key string) (reply interface{}, err error)
	SPopN(key string, count int64) (reply interface{}, err error)
	SRandMember(key string) (reply interface{}, err error)
	SRandMemberN(key string, count int64) (reply interface{}, err error)
	SRem(key string, members ...interface{}) (reply interface{}, err error)
	SUnion(keys ...string) (reply interface{}, err error)
	SUnionStore(destination string, keys ...string) (reply interface{}, err error)
	ZAdd(key string, members ...redis.Z) (reply interface{}, err error)
	ZAddNX(key string, members ...redis.Z) (reply interface{}, err error)
	ZAddXX(key string, members ...redis.Z) (reply interface{}, err error)
	ZAddCh(key string, members ...redis.Z) (reply interface{}, err error)
	ZAddNXCh(key string, members ...redis.Z) (reply interface{}, err error)
	ZAddXXCh(key string, members ...redis.Z) (reply interface{}, err error)
	ZIncr(key string, member redis.Z) (reply interface{}, err error)
	ZIncrNX(key string, member redis.Z) (reply interface{}, err error)
	ZIncrXX(key string, member redis.Z) (reply interface{}, err error)
	ZCard(key string) (reply interface{}, err error)
	ZCount(key, min, max string) (reply interface{}, err error)
	ZIncrBy(key string, increment float64, member string) (reply interface{}, err error)
	ZInterStore(destination string, store redis.ZStore, keys ...string) (reply interface{}, err error)
	ZRange(key string, start, stop int64) (reply interface{}, err error)
	ZRangeWithScores(key string, start, stop int64) (reply interface{}, err error)
	ZRangeByScore(key string, opt redis.ZRangeBy) (reply interface{}, err error)
	ZRangeByLex(key string, opt redis.ZRangeBy) (reply interface{}, err error)
	ZRangeByScoreWithScores(key string, opt redis.ZRangeBy) (reply interface{}, err error)
	ZRank(key, member string) (reply interface{}, err error)
	ZRem(key string, members ...interface{}) (reply interface{}, err error)
	ZRemRangeByRank(key string, start, stop int64) (reply interface{}, err error)
	ZRemRangeByScore(key, min, max string) (reply interface{}, err error)
	ZRemRangeByLex(key, min, max string) (reply interface{}, err error)
	ZRevRange(key string, start, stop int64) (reply interface{}, err error)
	ZRevRangeWithScores(key string, start, stop int64) (reply interface{}, err error)
	ZRevRangeByScore(key string, opt redis.ZRangeBy) (reply interface{}, err error)
	ZRevRangeByLex(key string, opt redis.ZRangeBy) (reply interface{}, err error)
	ZRevRangeByScoreWithScores(key string, opt redis.ZRangeBy) (reply interface{}, err error)
	ZRevRank(key, member string) (reply interface{}, err error)
	ZScore(key, member string) (reply interface{}, err error)
	ZUnionStore(dest string, store redis.ZStore, keys ...string) (reply interface{}, err error)

	Command() (reply interface{}, err error)
}
