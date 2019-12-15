package pub_sub_server

import (
	"github.com/docker/docker/pkg/pubsub"
	protos_pubsub "github.com/hyperledger/fabric/protos/pubsub"
	"golang.org/x/net/context"
	"time"
)

const (
	PublishTimeOut = 100 * time.Millisecond
	PublishBuffer  = 1024 * 1024
)

type PubSubServer struct {
	pub *pubsub.Publisher
}

func (p *PubSubServer) Publish(ctx context.Context, args *protos_pubsub.DistributeList) (*protos_pubsub.String, error) {
	p.pub.Publish(args)
	return &protos_pubsub.String{}, nil
}

func (p *PubSubServer) Subscribe(channelId *protos_pubsub.String, stream protos_pubsub.PubSubService_SubscribeServer) error {
	ch := p.pub.SubscribeTopic(func(v interface{}) bool {
		if key, ok := v.(*protos_pubsub.DistributeList); ok {
			if key.GetChannelId() == channelId.GetValue() {
				return true
			}
		}
		return false
	})

	for v := range ch {
		if err := stream.Send(v.(*protos_pubsub.DistributeList)); err != nil {
			return err
		}
	}

	return nil
}

func NewPubsubService() *PubSubServer {
	return &PubSubServer{
		pub: pubsub.NewPublisher(PublishTimeOut, PublishBuffer),
	}
}
