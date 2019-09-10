package redis

import (
	"github.com/gomodule/redigo/redis"
	LOGGER "github.yn.com/ext/common/logger"
//	"fmt"
)

var (
    url string
	pwd string
	pool *redis.Pool
)

func Startup(u,p string) {
	pool = redisPool(u,p)
	if pool == nil {	
		LOGGER.Error("pool == nil, init failed");
		return
    }
    url = u
    pwd = p
}

func SetValue(key string, value string) bool{
    c := pool.Get()
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
    c := pool.Get()
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
    c := pool.Get()
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
