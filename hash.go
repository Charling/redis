package redis

import (
	"github.com/gomodule/redigo/redis"
	LOGGER "github.yn.com/ext/common/logger"
//	"fmt"
)

//删除整个表
func Del(key string) bool {
	c := pool.Get()
    if c == nil{
        return false
    }
    defer c.Close()

    exit, err := redis.Bool(c.Do("DEL", key))
    if err != nil {
        LOGGER.Error("Del error: %v.", err)
        return false
    } else {
        return exit
    }
}

//HDEL key field1 [field2]:删除一个或多个哈希表字段
func Hdel(key string) bool {
    c := pool.Get()
    if c == nil{
        return false
    }
    defer c.Close()

    exit, err := redis.Bool(c.Do("HDEL", key))
    if err != nil {
        LOGGER.Error("error: %v.", err)
        return false
	} 
	return exit
}

//HEXISTS key field:查看哈希表 key 中，指定的字段是否存在。
func Hexists(key,field string) bool {
	c := pool.Get()
    if c == nil{
        return false
    }
    defer c.Close()

    exit, err := redis.Bool(c.Do("HEXISTS", key, field))
    if err != nil {
        LOGGER.Error("error: %v.", err)
        return false
	} 
	return exit
}

//HGET key field:获取存储在哈希表中指定字段的值。
func Hget(key,field string) string {
	c := pool.Get()
    if c == nil{
        return ""
    }
    defer c.Close()

    value, err := redis.String(c.Do("HGET", key,field))
    if err != nil {
        LOGGER.Error("redis Hget failed key:%s  err:%+v \n", key, err)
        return ""
    }

    return value
}

//HGETALL key:获取在哈希表中指定 key 的所有字段和值
func Hgetall(key string) []interface{} {
	c := pool.Get()
    if c == nil{
		return nil
    }
    defer c.Close()

    value, err := redis.Values(c.Do("HGETALL", key))
    if err != nil {
        LOGGER.Error("redis HGETALL failed key:%s  err:%+v \n", key, err)
        return nil
    }

    return value
}

//HSET key field value:将哈希表 key 中的字段 field 的值设为 value 。
func Hset(key, field string, value interface{}) bool {
    c := pool.Get()
    if c == nil{
		return false
    }
    defer c.Close()

    _, err := redis.Bool(c.Do("HSET", key, field, value))
    if err != nil {
        LOGGER.Error("redis HSET failed key:%s  err:%+v \n", key, err)
        return false
    }

    return true
}

//HMSET key [field value] [field value] ...：将哈希表 key 中的字段 field 的值设为 value 。 
//example:HMSET runoobkey name "redis tutorial" description "redis basic commands for caching" likes 20 visitors 23000
func Hmset(key string, object interface{}) bool {
	c := pool.Get()
    if c == nil{
		return false
    }
    defer c.Close()

    _, err := redis.String(c.Do("HMSET", redis.Args{key}.AddFlat(object)...))
    if err != nil {
        LOGGER.Error("redis Hset failed key:%s  err:%+v \n", key, err)
        return false
    }

    return true
}