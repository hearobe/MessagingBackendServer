package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/kitex_gen/rpc"
	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/kitex_gen/rpc/imservice"
	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/proto_gen/api"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/kitex/client"
	etcd "github.com/kitex-contrib/registry-etcd"
)

var cli imservice.Client

func main() {
	r, err := etcd.NewEtcdResolver([]string{"etcd:2379"})
	if err != nil {
		log.Fatal(err)
	}
	cli = imservice.MustNewClient("demo.rpc.server",
		client.WithResolver(r),
		client.WithRPCTimeout(1*time.Second), // was changed from 1 to 10 seconds
		client.WithHostPorts("rpc-server:8888"),
	)

	h := server.Default(server.WithHostPorts("0.0.0.0:8080"))

	h.GET("/ping", func(c context.Context, ctx *app.RequestContext) {
		ctx.JSON(consts.StatusOK, utils.H{"message": "pong"})
	})

	h.POST("/api/send", sendMessage)
	h.GET("/api/pull", pullMessage)

	h.Spin()
}

func sendMessage(ctx context.Context, c *app.RequestContext) {
	sender, hasSender := c.GetQuery("sender")
	receiver, hasReceiver := c.GetQuery("receiver")
	text, hasText := c.GetQuery("text")
	if !hasSender || !hasReceiver || !hasText {
		c.String(consts.StatusBadRequest, "Failed to parse request body")
	}

	chatId := ""
	if strings.Compare(sender, receiver) <= 0 {
		chatId = sender + ":" + receiver
	} else {
		chatId = receiver + ":" + sender
	}

	resp, err := cli.Send(ctx, &rpc.SendRequest{
		Message: &rpc.Message{
			Chat:     chatId,
			Text:     text,
			Sender:   sender,
			SendTime: time.Now().Unix(),
		},
	})
	if err != nil {
		c.String(consts.StatusInternalServerError, err.Error())
	} else if resp.Code != 0 {
		c.String(consts.StatusInternalServerError, resp.Msg)
	} else {
		c.Status(consts.StatusOK)
	}
}

func pullMessage(ctx context.Context, c *app.RequestContext) {
	chatInput, hasChat := c.GetQuery("chat")
	startInput, hasStart := c.GetQuery("start") //start should be 0-indexed
	countInput, hasCount := c.GetQuery("count") // number of messages expected
	reverseInput, hasReverse := c.GetQuery("reverse")
	if !hasChat {
		c.String(consts.StatusBadRequest, "Chat ID needed")
	}

	var err error
	count := 10
	reverse := false
	if hasCount {
		count, err = strconv.Atoi(countInput)
	}
	if hasReverse && reverseInput == "true" {
		reverse = true
	}
	start := 0
	if hasStart {
		start, err = strconv.Atoi(startInput)
	} else if hasReverse {
		start = -1
	}

	resp, err := cli.Pull(ctx, &rpc.PullRequest{
		Chat:    chatInput,
		Cursor:  int64(start),
		Limit:   int32(count),
		Reverse: &reverse,
	})
	if err != nil {
		c.String(consts.StatusInternalServerError, err.Error())
		return
	} else if resp.Code == 0 {
		c.String(consts.StatusInternalServerError, resp.Msg)
		return
	}

	messages := make([]*api.Message, 0, len(resp.Messages))
	for _, msg := range resp.Messages {
		messages = append(messages, &api.Message{
			Chat:     msg.Chat,
			Text:     msg.Text,
			Sender:   msg.Sender,
			SendTime: msg.SendTime,
		})
	}

	c.JSON(consts.StatusOK, &api.PullResponse{
		Messages:   messages,
		HasMore:    resp.GetHasMore(),
		NextCursor: resp.GetNextCursor(),
	})
}
