package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
	"github.com/redis/go-redis/v9"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

type Message struct {
	Chat     string
	Text     string
	Sender   string
	SendTime int64
}

func NewMessage(m *rpc.Message) *Message {
	return &Message{
		Chat:     m.Chat,
		Text:     m.Text,
		Sender:   m.Sender,
		SendTime: m.SendTime,
	}
}

func (m Message) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	log.Println(ctx)
	resp := rpc.NewSendResponse()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "assignment_demo_2023-redis-server-1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	newMsg := NewMessage(req.Message)

	err := rdb.LPush(ctx, newMsg.Chat, newMsg).Err()
	if err != nil {
		panic(err)
	}

	resp.Code, resp.Msg = 200, req.Message.Text
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	resp := rpc.NewPullResponse()
	log.SetOutput(os.Stdout)
	rdb := redis.NewClient(&redis.Options{
		Addr:     "assignment_demo_2023-redis-server-1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	var err error
	var redisMessages []string
	if *req.Reverse {
		for i := req.Cursor; i >= req.Cursor-int64(req.Limit)+1; i-- {
			val, err := rdb.LRange(ctx, req.Chat, req.Cursor, req.Cursor+int64(req.Limit)-1).Result()
			if err != nil {
				panic(err)
			}
			redisMessages = append(redisMessages, val...)
		}
	} else {
		val, err := rdb.LRange(ctx, req.Chat, req.Cursor, req.Cursor+int64(req.Limit)-1).Result()
		if err != nil {
			panic(err)
		}
		redisMessages = val
	}
	if err != nil {
		panic(err)
	}
	if len(redisMessages) == 0 {
		resp.Code, resp.Msg = 404, ""
	}
	log.Println(redisMessages)

	messages := make([]*rpc.Message, len(redisMessages))
	for i := 0; i < len(redisMessages); i++ {
		err = json.Unmarshal([]byte(redisMessages[i]), &messages[i])
	}
	if err != nil {
		panic(err)
	}
	log.Println(messages)

	resp.Code, resp.Messages = 200, messages
	return resp, nil
}
