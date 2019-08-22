package redis

import (
	"github.com/gomodule/redigo/redis"
	"time"
)

type IoRedis struct {
	conn *redis.Pool
}

var (
	Pool *IoRedis
)

func RedisPool(host string) *redis.Pool {
	conn := &redis.Pool{
        //最大空闲
        MaxIdle: 100,
        //最大活动量
        MaxActive: 30,
        //空闲超时
        IdleTimeout: 60 * time.Second,
        Dial: func() (redis.Conn, error) {
            c, err := redis.Dial("tcp", host)
            if err != nil {
                return nil, err
            }
            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
	}
	
	return conn
}

func (io *IoRedis) Get() *redis.Pool {
	return io.conn
}

