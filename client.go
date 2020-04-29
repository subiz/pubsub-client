package client

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/subiz/header"
	pb "github.com/subiz/header/pubsub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
)

type Pubsub struct {
	sync.Mutex

	clients  []header.PubsubClient
	service  string
	maxNodes int
}

func NewPubsubClient(service string, maxNodes int) *Pubsub {
	return &Pubsub{
		service:  service,
		maxNodes: maxNodes,
		clients:  make([]header.PubsubClient, maxNodes),
	}
}

func (me *Pubsub) PublishAsync(topics, userids, neguserids []string, payload []byte) {
	go func() {
		if err := me.Publish(topics, userids, neguserids, payload); err != nil {
			fmt.Println("Publish message error", err.Error())
		}
	}()
}

func (me *Pubsub) Publish(topics, userids, neguserids []string, payload []byte) error {
	if len(topics) == 0 {
		return nil
	}

	ctx := context.Background()
	wg := &sync.WaitGroup{}
	wg.Add(len(topics))
	var gerr error
	for _, topic := range topics {
		go func(topic string) {
			defer wg.Done()
			client, err := me.getPubsubClient(topic)
			if err != nil {
				gerr = err
				return
			}

			mes := me.createPublishMesasge([]string{topic}, userids, neguserids, payload)
			if _, err := client.Publish(ctx, mes); err != nil {
				gerr = err
				return
			}
		}(topic)
	}
	wg.Wait()
	return gerr
}

func (me *Pubsub) createPublishMesasge(topics, userids, neguserids []string, payload []byte) *pb.PublishMessage {
	mes := &pb.PublishMessage{}
	mes.Payload = payload
	mes.Topics = topics

	return mes
}

func (me *Pubsub) Subscribe(sub *pb.Subscription) error {
	ctx := context.Background()
	wg := &sync.WaitGroup{}
	wg.Add(len(sub.GetTopics()))
	var gerr error
	for _, topic := range sub.GetTopics() {
		go func(topic string) {
			defer wg.Done()
			client, err := me.getPubsubClient(topic)
			if err != nil {
				gerr = err
				return
			}
			s := proto.Clone(sub).(*pb.Subscription)
			s.Topics = []string{topic}
			if _, err := client.Subscribe(ctx, s); err != nil {
				gerr = err
				return
			}
		}(topic)
	}
	wg.Wait()
	return gerr
}

func (me *Pubsub) Unsubscribe(sub *pb.Subscription) error {
	ctx := context.Background()
	wg := &sync.WaitGroup{}
	wg.Add(len(sub.GetTopics()))
	var gerr error
	for _, topic := range sub.GetTopics() {
		go func(topic string) {
			defer wg.Done()
			client, err := me.getPubsubClient(topic)
			if err != nil {
				gerr = err
				return
			}
			s := proto.Clone(sub).(*pb.Subscription)
			s.Topics = []string{topic}
			if _, err := client.Subscribe(ctx, s); err != nil {
				gerr = err
				return
			}
		}(topic)
	}
	wg.Wait()
	return gerr
}

func (me *Pubsub) getPubsubClient(key string) (header.PubsubClient, error) {
	no := int(crc32.ChecksumIEEE([]byte(key))) % me.maxNodes

	me.Lock()
	defer me.Unlock()

	if len(me.clients) > 0 && me.clients[no] != nil {
		return me.clients[no], nil
	}

	parts := strings.SplitN(me.service, ":", 2)
	name, port := parts[0], parts[1]

	// address: [pod name] + "." + [service name] + ":" + [pod port]
	conn, err := dialGrpc(name + "-" + strconv.Itoa(no) + "." + name + ":" + port)
	if err != nil {
		fmt.Println("unable to connect to pubsub service", err)
		return nil, err
	}
	me.clients[no] = header.NewPubsubClient(conn)

	return me.clients[no], nil
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
