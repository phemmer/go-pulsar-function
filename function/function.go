package function

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/conf"
	pb "github.com/apache/pulsar/pulsar-function-go/pb"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/phemmer/errors"
	"github.com/phemmer/go-pulsar-function/bookkeeper/kv"
	"github.com/phemmer/go-pulsar-function/bookkeeper/proto/storage"
	"github.com/phemmer/go-pulsar-function/bookkeeper/proto/stream"

	"google.golang.org/grpc"
)

type ServerConfig struct {
	Args conf.Conf

	// Properties is shared amongst the producers & consumers. If you wish to override just one, copy and replace.
	Properties map[string]string
	// ProducerOptions is shared amongst the non-log producers.
	ProducerOptions pulsar.ProducerOptions
	ConsumerOptions []pulsar.ConsumerOptions
	LogOptions pulsar.ProducerOptions
}

// GetFunctionServerConfig returns the configuration passed from pulsar.
// The returned config can be optionally modified, and passed to NewServer().
func GetFunctionServerConfig() ServerConfig {
	sc := ServerConfig{}

	sc.Args.GetConf()

	sc.Properties = map[string]string{
		"application": "pulsar-function",
		"id":          sc.Args.Tenant + "/" + sc.Args.NameSpace + "/" + sc.Args.Name,
		"instance_id": strconv.Itoa(sc.Args.InstanceID),
	}
	if sc.Args.SinkSpecTopic != "" {
		sc.ProducerOptions = pulsar.ProducerOptions{
			Topic:                   sc.Args.SinkSpecTopic,
			Properties:              sc.Properties,
			CompressionType:         pulsar.LZ4,
			BatchingMaxPublishDelay: time.Millisecond * 10,
		}
	}

	//TODO figure out how multiple consumer topics are passed
	co := pulsar.ConsumerOptions{
		Properties:        sc.Properties,
		SubscriptionName:  sc.Properties["id"],
		ReceiverQueueSize: int(sc.Args.ReceiverQueueSize),
		Type: map[pb.SubscriptionType]pulsar.SubscriptionType{
				pb.SubscriptionType_SHARED: pulsar.Shared,
				pb.SubscriptionType_FAILOVER: pulsar.Failover,
			}[pb.SubscriptionType(sc.Args.SubscriptionType)],
	}
	if sc.Args.IsRegexPatternSubscription {
		co.TopicsPattern = sc.Args.SourceSpecTopic
	} else {
		co.Topic = sc.Args.SourceSpecTopic
	}
	sc.ConsumerOptions = append(sc.ConsumerOptions, co)

	if sc.Args.LogTopic != "" {
		sc.LogOptions = pulsar.ProducerOptions{
			Topic:                   sc.Args.LogTopic,
			Properties:              sc.Properties,
			CompressionType:         pulsar.LZ4,
			BatchingMaxPublishDelay: time.Millisecond * 100,
		}
	}

	return sc
}

type Server struct {
	Conf ServerConfig

	healthTimer *time.Timer

	// Calling this will cancel the context of any workers we've spawned (i.e. shut them down).
	ctxCancel func()
	// Done can be used to listen for a shutdown signal.
	Done func() <-chan struct{}

	Client    pulsar.Client
	Producer  pulsar.Producer
	Consumers []pulsar.Consumer
	LogProducer pulsar.Producer

	bkClient *kv.Client
	kvTable  *kv.Table
}

func NewServer(ctx context.Context, conf ServerConfig) (*Server, error) {
	s := &Server{
		Conf: conf,
	}

	ctx, s.ctxCancel = context.WithCancel(ctx)
	s.Done = ctx.Done

	if s.Conf.Args.ExpectedHealthCheckInterval > 0 {
		s.healthTimer = time.AfterFunc(
			time.Second*time.Duration(s.Conf.Args.ExpectedHealthCheckInterval)*3,
			s.ctxCancel)
	}

	var err error
	if s.Client, err = pulsar.NewClient(pulsar.ClientOptions{URL: s.Conf.Args.PulsarServiceURL}); err != nil {
		s.Stop()
		return nil, err
	}

	if s.Conf.Args.LogTopic != "" {
		s.LogProducer, err = s.Client.CreateProducer(s.Conf.LogOptions)
		if err != nil {
			s.Stop()
			return nil, err
		}
	}

	if s.Conf.ProducerOptions.Topic != "" {
		s.Producer, err = s.Client.CreateProducer(s.Conf.ProducerOptions)
		if err != nil {
			s.Stop()
			return nil, err
		}
	}

	for _, co := range s.Conf.ConsumerOptions {
		cons, err := s.Client.Subscribe(co)
		if err != nil {
			s.Stop()
			return nil, err
		}

		s.Consumers = append(s.Consumers, cons)
	}

	if err = (*grpcServer)(s).ListenAndServe(ctx); err != nil {
		s.Stop()
		return nil, err
	}

	return s, nil
}

func (s *Server) Stop() {
	if s.Producer != nil {
		s.Producer.Flush()
	}
	if s.ctxCancel != nil {
		s.ctxCancel()
	}
	if s.Client != nil {
		s.Client.Close()
	}
	//TODO put in a mechanism to wait for workers to shut down before returning
}

func (s *Server) Logf(format string, args ...interface{}) {
	if s.LogProducer == nil {
		fmt.Printf(format + "\n", args)
		return
	}

	//TODO I don't like using Background. We should probably hold onto the context used in NewServer().
	msg := pulsar.ProducerMessage{Payload: []byte(fmt.Sprintf(format, args))}
	s.LogProducer.SendAsync(context.Background(), &msg, s.loggerError)
}

func (s *Server) loggerError(msgID pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
	fmt.Fprintf(os.Stderr, "Error writing to log topic: %s\nLog: %s", err, msg.Payload)
}

func (s *Server) GetBKClient(ctx context.Context) (*kv.Client, error) {
	if s.bkClient != nil {
		return s.bkClient, nil
	}

	pulsarURL := s.Conf.Args.PulsarServiceURL
	u, err := url.Parse(pulsarURL)
	if err != nil { return nil, err }
	host, _, _ := net.SplitHostPort(u.Host)

	ns := strings.Replace(s.Conf.Args.Tenant + "_" + s.Conf.Args.NameSpace, "-", "_", -1)
	s.bkClient, err = kv.NewClient(ctx, host+":4181", ns)
	return s.bkClient, err
}

func (s *Server) GetKVTable(ctx context.Context) (*kv.Table, error) {
	if s.kvTable != nil { return s.kvTable, nil }

	cl, err := s.GetBKClient(ctx)
	if err != nil {
		return nil, err
	}

	s.kvTable, err = cl.GetTable(ctx, s.Conf.Args.Name)
	if err != nil && errors.Contains(err, kv.StorageError(storage.StatusCode_STREAM_NOT_FOUND)) {
		s.kvTable, err = s.createKVTable(ctx)
		err = errors.F(err, "creating table")
	}

	return s.kvTable, err
}

func (s *Server) createKVTable(ctx context.Context) (*kv.Table, error) {
	cl, err := s.GetBKClient(ctx)
	if err != nil {
		return nil, err
	}

	// https://github.com/apache/pulsar/blob/v2.5.0/pulsar-functions/instance/src/main/java/org/apache/pulsar/functions/instance/JavaInstanceRunnable.java#L337-L340
	s.kvTable, err = cl.CreateTable(ctx, s.Conf.Args.Name, &stream.StreamConfiguration{
		MinNumRanges:     4,
		InitialNumRanges: 4,
		StorageType:      stream.StorageType_TABLE,
		KeyType:          stream.RangeKeyType_HASH,
		RetentionPolicy: &stream.RetentionPolicy{
			TimePolicy: &stream.TimeBasedRetentionPolicy{
				RetentionMinutes: -1,
			},
		},
	})
	if err != nil && errors.Contains(err, kv.StorageError(storage.StatusCode_STREAM_EXISTS)) {
		// likely someone else tried to create table at same time we did. Try get
		s.kvTable, err = cl.GetTable(ctx, s.Conf.Args.Name)
	}

	return s.kvTable, err
}

// grpcServer is an alias of Server so the user doesn't have to see & worry about these methods.
type grpcServer Server

func (gs *grpcServer) ListenAndServe(ctx context.Context) error {
	if gs.Conf.Args.Port == 0 {
		return nil
	}

	l, err := net.Listen("tcp", "127.0.0.1:" + strconv.Itoa(gs.Conf.Args.Port))
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	pb.RegisterInstanceControlServer(srv, gs)
	go srv.Serve(l)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	return nil
}

func (gs *grpcServer) GetFunctionStatus(ctx context.Context, _ *empty.Empty) (*pb.FunctionStatus, error) {
	return nil, nil
}

func (gs *grpcServer) GetAndResetMetrics(ctx context.Context, _ *empty.Empty) (*pb.MetricsData, error) {
	return nil, nil
}

func (gs *grpcServer) ResetMetrics(ctx context.Context, _ *empty.Empty) (*empty.Empty, error) {
	return nil, nil
}

func (gs *grpcServer) GetMetrics(ctx context.Context, _ *empty.Empty) (*pb.MetricsData, error) {
	return nil, nil
}

func (gs *grpcServer) HealthCheck(ctx context.Context, _ *empty.Empty) (*pb.HealthCheckResult, error) {
	if gs.healthTimer == nil {
		return &pb.HealthCheckResult{Success: true}, nil
	}

	ok := gs.healthTimer.Stop()
	if ok {
		gs.healthTimer.Reset(time.Second * time.Duration(gs.Conf.Args.ExpectedHealthCheckInterval) * 3)
	}
	return &pb.HealthCheckResult{Success: ok}, nil
}
