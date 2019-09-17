package redis

import (
	"github.com/gomodule/redigo/redis"
	LOGGER "github.yn.com/ext/common/logger"
//	"fmt"
)

type RedisClient struct {
	pool *redis.Pool
}

var (
    url string
	pwd string
	redisClient *RedisClient
)

func Startup(u,p string) {
	redisClient = redisPool(u,p)
	if redisClient == nil {	
		LOGGER.Error("redisClient == nil, init failed");
		return
    }
    url = u
    pwd = p
}

func (r *RedisClient) Close() error {
	err := r.pool.Close()
	return err
}

func IoRedis() *redis.Conn {
    c := redisClient.pool.Get()
    if c == nil{
        return nil
    }
    return &c
}

func SetValue(key string, value string) bool{
    c := redisClient.pool.Get()
    if c == nil{
        return false
    }
    defer c.Close()

    _, err := c.Do("SET", key, value)
    if err != nil{
        LOGGER.Error("redis set failed key:%s value:%s err:%+v \n", key, value, err)
        return false
    }
    return true
}

func GetValue(key string) string{
    c := redisClient.pool.Get()
    if c == nil{
        return ""
    }
    defer c.Close()

    value, err := redis.String(c.Do("GET", key))
    if err != nil {
        LOGGER.Error("redis set failed key:%s  err:%+v \n", key, err)
        return ""
    }

    return value
}

func IsExitKey(key string) bool {
    c := redisClient.pool.Get()
    if c == nil{
        return false
    }
    defer c.Close()

    exit, err := redis.Bool(c.Do("EXISTS", key))
    if err != nil {
        LOGGER.Error("error: %v.", err)
        return false
    } else {
        return exit
    }
}
