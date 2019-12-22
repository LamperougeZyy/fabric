package common

import (
	"context"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/pubsub"
	"google.golang.org/grpc"
)

type SubscribeServiceServer struct {
}

func (s *SubscribeServiceServer) NotifySubscriber(ctx context.Context, req *peer.SubscribeRequest) (*peer.SubscribeResponse, error) {
	logger.Infof("Get subscriber create info: %+v", req)
	conn, err := grpc.Dial(req.OrdererEndPoint, grpc.WithInsecure())
	if err != nil {
		logger.Errorf("Dail order pubsub server failed! %s", err.Error())
		return nil, err
	}

	client := pubsub.NewPubSubServiceClient(conn)
	stream, err := client.Subscribe(context.Background(), &pubsub.String{Value: req.ChannelId})
	if err != nil {
		logger.Errorf("Get subscribe stream from orderer error: %s", err.Error())
		return nil, err
	}
	// zyy: 开一个新的协程持续获取来自pubsub server的消息
	go func() {
		for {
			distributeList, err := stream.Recv()
			if err != nil {
				logger.Warnf("Stream receive distribute error: %s", err.Error())
			} else {
				logger.Debugf("Oh Yeah! First Stage Complete! Get distribute list: %+v", distributeList)
			}
		}
	}()

	return &peer.SubscribeResponse{
		Msg: "Bootstrap subscriber success",
	}, nil
}

func NewSubscribeServiceServer() *SubscribeServiceServer {
	return &SubscribeServiceServer{}
}
