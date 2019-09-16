package client

import (
	"bytes"
	"context"
	"fmt"
	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	pb "github.com/subiz/header/pubsub"
	"github.com/subiz/kafka"
	"github.com/willf/bloom"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"time"
)

type Pubsub struct {
	client header.PubsubClient
	es     *kafka.Publisher
}

func (p *Pubsub) PublishAsync(topics, userids, neguserids []string, payload []byte) {
	key := topics[0]
	mes := p.createPublishMesasge(topics, userids, neguserids, payload)
	p.es.PublishAsync(header.E_PubsubRequested.String(), mes, -1, key)
}

func (p *Pubsub) Publish(topics, userids, neguserids []string, payload []byte) {
	if len(topics) == 0 {
		return
	}
	key := topics[0]
	mes := p.createPublishMesasge(topics, userids, neguserids, payload)
	p.es.Publish(header.E_PubsubRequested.String(), mes, -1, key)
}

func (p *Pubsub) createPublishMesasge(topics, userids, neguserids []string, payload []byte) *pb.PublishMessage {
	mes := &pb.PublishMessage{}
	mes.Payload = payload
	mes.Topics = topics

	if len(userids) > 0 {
		filter := bloom.New(300, 5)
		for _, userid := range userids {
			filter.Add([]byte(userid))
		}
		var userswriter bytes.Buffer
		filter.WriteTo(&userswriter)
		mes.UserIdsFilter = userswriter.Bytes()
	}

	if len(neguserids) > 0 {
		filter := bloom.New(300, 5)
		for _, userid := range neguserids {
			filter.Add([]byte(userid))
		}
		var neguserswriter bytes.Buffer
		filter.WriteTo(&neguserswriter)
		mes.NegUserIdsFilter = neguserswriter.Bytes()
	}
	mes.Ctx = &cpb.Context{SubTopic: header.E_PubsubPublish.String()}
	return mes
}

func (p *Pubsub) Subscribe(sub *pb.Subscription) error {
	ctx := context.Background()
	_, err := p.client.Subscribe(ctx, sub)
	return err
}

func (p *Pubsub) Unsubscribe(sub *pb.Subscription) error {
	ctx := context.Background()
	_, err := p.client.Subscribe(ctx, sub)
	return err
}

func dialPubsubService(service string) (header.PubsubClient, error) {
	conn, err := dialGrpc(service)
	if err != nil {
		fmt.Println("unable to connect to pubsub service", err)
		return nil, err
	}
	return header.NewPubsubClient(conn), nil
}

func dialGrpc(service string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	// Enabling WithBlock tells the client to not give up trying to find a server
	opts = append(opts, grpc.WithBlock())
	// However, we're still setting a timeout so that if the server takes too long, we still give up
	opts = append(opts, grpc.WithTimeout(10*time.Second))
	opts = append(opts, grpc.WithBalancerName(roundrobin.Name))
	return grpc.Dial(service, opts...)
}

func NewPubsubClient(brokers []string, service string) *Pubsub {
	c, err := dialPubsubService(service)
	if err != nil {
		panic(err)
	}
	return &Pubsub{client: c, es: kafka.NewPublisher(brokers)}
}
