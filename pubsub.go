package redis

import (
	"github.com/gomodule/redigo/redis"
	"time"
	LOGGER "yn.com/ext/common/logger"
	"yn.com/ext/common/gomsg"
	Proto "yn.com/ext/common/proto"
	"github.com/golang/protobuf/proto"
	"fmt"
)

type PubSubEvent func(channel string, bytes []byte)

type stPubsubRedis struct {
	handler PubSubEvent
	url string
	pool *redis.Pool
}

var (
	pubsub stPubsubRedis
)

func Sub(url string, channles[]string, e PubSubEvent) {
	pubsub.pool = redisPool(url)
	pubsub.url = url
	pubsub.handler = e

	register(channles)

	go subPublisherEvents(channles)
}

func Publish(channel string, id int64, ops int32, data []byte) error {
	conn := pubsub.pool.Get()
	defer conn.Close()
	
	msg := &Proto.Message {
		PlayerId: proto.Int64(id),
		Ops: proto.Int32(ops),
		Data: data,
	}

	res, err := proto.Marshal(msg)
	if err != nil {
		e := fmt.Sprintf("Mashal data error %v", err)
		LOGGER.Error(e)
		return err
	}

	_, err = conn.Do("PUBLISH", channel, res)
	if err != nil {
        LOGGER.Error("Publish err(%v).", err)
		return err
	}
	return nil
}

func register(channelList []string) {
	for _, c := range channelList{
		LOGGER.Error("registerChannel(%s).", c)
		psc := redis.PubSubConn{pubsub.pool.Get()}
		err := psc.Subscribe(c)
		if err != nil {
			LOGGER.Error(err.Error())
			continue
		}
	}
}

func subPublisherEvents(channles[]string) {
	defer gomsg.Recover()
	psc := redis.PubSubConn{pubsub.pool.Get()}

	quit := false
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			pubsub.handler(v.Channel, v.Data)

		case error:
			LOGGER.Error("redis subPublisherEvents channel exited, try to restart. (%v)", v)
			<-time.After(5 * time.Second)

			pubsub.pool.Get().Close()
			Sub(pubsub.url, channles, pubsub.handler)
			quit = true
		}

		if quit {
			break
		}
	}
}
