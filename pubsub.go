package redis

import (
	"github.com/gomodule/redigo/redis"
	"time"
	LOGGER "github.yn.com/ext/common/logger"
	"github.yn.com/ext/common/gomsg"
	Proto "github.yn.com/ext/common/proto"
	"github.com/golang/protobuf/proto"
	"fmt"
)

type MsgHandler func(int64, []byte, int32)

var (
	handler map[int32]MsgHandler
	channles[]string
)

func Subscribe(url,pwd string, chans[]string, msg *map[int32]MsgHandler) {
	handler = *msg
	channles = chans

	subPublisherEvents()
}

func reSubscribe() {
	subPublisherEvents()
}

func Publish(channel string, id int64, ops int32, data []byte) error {
	conn := redisClient.pool.Get()
	defer conn.Close()
	
	msg := &Proto.Message {
		PlayerId: proto.Int64(id),
		Ops: proto.Int32(ops),
		Data: data,
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
	defer gomsg.Recover()
	psc := redis.PubSubConn{Conn: redisClient.pool.Get()}

	arrlen := len(channles)
	for i:=0;i<arrlen;i++ {	
		err := psc.Subscribe(channles[i])
		if err != nil{
			LOGGER.Error("redis Subscribe %s error.", channles[i])
			return
		}
	}
	done := make(chan string, 1)

	go func() {
		defer psc.Close()
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				if channelIsOk(v.Channel) == true {
					msg := &Proto.Message{}
					err := proto.Unmarshal(v.Data, msg)
					if err == nil {
						handler := handler[*msg.Ops]
						if handler != nil {
							handler(*msg.PlayerId, msg.Data, *msg.Size)
						}
					}  else {
						LOGGER.Error(fmt.Sprintf("redis subPublisherEvents error: %v.", err))
					}
				}

			case error:
				done <- fmt.Sprintf("redis subPublisherEvents channel exited, try to restart. (%v)", v)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case err := <- done:
				LOGGER.Error(err)

				<-time.After(60 * time.Second)
				redisClient.pool.Get().Close()
				LOGGER.Info("try connect redis ....")
				Startup(url, pwd)
				reSubscribe()
				return
			}
		}
	}()
}

