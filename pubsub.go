package redis

import (
	"github.com/gomodule/redigo/redis"
	"time"
	LOGGER "github.yn.com/ext/common/logger"
	//"github.yn.com/ext/common/gomsg"
	Proto "github.yn.com/ext/common/proto"
	"github.com/golang/protobuf/proto"
	"fmt"
)

type PubSubEvent func(channel string, bytes []byte)
type MsgHandler func(int64, []byte, int32)

type stPubsubRedis struct {
	url string
	pwd string
	client *RedisClient
	handler PubSubEvent
}

var (
	pubsub stPubsubRedis 
	channles []string
	done chan string
	handers map[int32]MsgHandler
)

func Subscribe(url,pwd string, chans[]string, msg *map[int32]MsgHandler) {
	pubsub.url = url
	pubsub.pwd = pwd
	pubsub.client = redisPool(url,pwd)
	if pubsub.client == nil {	
		LOGGER.Error("pubsub.client == nil, init failed");
		return
    }
	handers = *msg
	if done == nil {
		done = make(chan string)
	}

	pubsub.handler = func(channel string, bytes []byte) {
		arrlen := len(channles)
		find := false
		for i:=0;i<arrlen;i++ {
			if channles[i] == channel {
				find = true
				break
			}
		}
		if find == false {
			return
		}
		msg := &Proto.Message{}
		err := proto.Unmarshal(bytes, msg)
		if err == nil {
			handler := handers[*msg.Ops]
			if handler != nil {
				handler(*msg.PlayerId, msg.Data, *msg.Size)
			}
		}  else {
			LOGGER.Error(fmt.Sprintf("redis Unmarshal error: %v.", err))
		}
	}
	channles = chans
	arrlen := len(channles)
	LOGGER.Info("Subscribe arrlen:%d", arrlen)
	for i:=0;i<arrlen;i++ {
		LOGGER.Info("channles idx:%d,%s", i,channles[i])
	}

	go subPublisherEvents()
	go reSubscribe()
}

func reSubscribe() {
	for {
		select {
		case err := <- done:
			LOGGER.Error(err)
			<-time.After(10 * time.Second)

			pubsub.client.pool.Get().Close()

			LOGGER.Info("try connect redis ....")
			Subscribe(pubsub.url,pubsub.pwd, channles, &handers)
			return
			
		}
	}
}

func Publish(channel string, id int64, ops int32, data []byte) error {
	conn := pubsub.client.pool.Get()
	defer conn.Close()

	msg := &Proto.Message {
		PlayerId: proto.Int64(id),
		Ops: proto.Int32(ops),
		Data: data,
		Size: proto.Int32(int32(len(data))),
	}

	message, err := proto.Marshal(msg)
	if err != nil {
		e := fmt.Sprintf("Mashal data error %v", err)
		LOGGER.Error(e)
		return err
	}

	_, err = conn.Do("PUBLISH", channel, message)
	if err != nil {
        LOGGER.Error("Publish err(%v).", err)
		return err
	}
	return nil
}

func channelIsOk(channel string) bool {
	arrlen := len(channles)
	for i:=0;i<arrlen;i++ {
		if channles[i] == channel {
			return true
		}
	}
	return false
}

func subPublisherEvents() {
	defer LOGGER.Recover()
	psc := redis.PubSubConn{Conn: pubsub.client.pool.Get()}

	arrlen := len(channles)
	for i:=0;i<arrlen;i++ {	
		err := psc.Subscribe(channles[i])
		if err != nil{
			done <- fmt.Sprintf("redis Subscribe %s error=%v.", channles[i], err)
			return
		}
	}

	defer psc.Close()
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			pubsub.handler(v.Channel, v.Data)

		case error:
			done <- fmt.Sprintf("redis subPublisherEvents channel exited, try to restart. (%v)", v)
			return
		}
	}
}

