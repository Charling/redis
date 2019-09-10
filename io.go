package redis

import (
	"github.com/gomodule/redigo/redis"
    "time"
    "fmt"
)

func redisPool(host,pwd string) *redis.Pool {
	pool := &redis.Pool{
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
            if pwd != "" {
                //验证redis密码
                if _, authErr := c.Do("AUTH", pwd); authErr != nil {
                    return nil, fmt.Errorf("redis auth password error: %s", authErr)
                }
            }
            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
	}
	
	return pool
}
