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
)

func Sub(url,pwd string, channles[]string, msg *map[int32]MsgHandler) {
	handler = *msg

	register(channles)

	go subPublisherEvents(channles)
}

func Publish(channel string, id int64, ops int32, data []byte) error {
	conn := pool.Get()
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
		LOGGER.Info("registerChannel(%s).", c)
		psc := redis.PubSubConn{pool.Get()}
		err := psc.Subscribe(c)
		if err != nil {
			LOGGER.Error(err.Error())
			continue
		}
	}
}

func channelIsOk(channles []string, channel string) bool {
	arrlen := len(channles)
	for i:=0;i<arrlen;i++ {
		if channles[i] == channel {
			return true
		}
	}
	return false
}

func subPublisherEvents(channles []string) {
	defer gomsg.Recover()
	psc := redis.PubSubConn{pool.Get()}

	quit := false
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			if channelIsOk(channles, v.Channel) == true {
				msg := &Proto.Message{}
				err := proto.Unmarshal(v.Data, msg)
				if err == nil {
					handler := handler[*msg.Ops]
					if handler != nil {
						handler(*msg.PlayerId, msg.Data, *msg.Size)
					}
				}
			}

		case error:
			LOGGER.Error("redis subPublisherEvents channel exited, try to restart. (%v)", v)
			<-time.After(5 * time.Second)

			pool.Get().Close()
			Sub(url, pwd, channles, &handler)
			quit = true
		}

		if quit {
			break
		}
	}
}

