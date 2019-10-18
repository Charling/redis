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
	handers = *msg

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

	go subPublisherEvents()
	go reSubscribe()
}

func reSubscribe() {
	for {
		select {
		case err := <- done:
			LOGGER.Error(err)

			<-time.After(60 * time.Second)
			redisClient.pool.Get().Close()
			LOGGER.Info("try connect redis ....")
			Startup(pubsub.url, pubsub.pwd)
			go subPublisherEvents()
			return
		}
	}
}

func Publish(channel string, id int64, ops int32, data []byte) error {
	conn := redisClient.pool.Get()
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
	psc := redis.PubSubConn{Conn: redisClient.pool.Get()}

	arrlen := len(channles)
	for i:=0;i<arrlen;i++ {	
		err := psc.Subscribe(channles[i])
		if err != nil{
			LOGGER.Error("redis Subscribe %s error.", channles[i])
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

