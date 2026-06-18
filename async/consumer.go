package async

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	kafkapkg "github.com/velonetics/velonetics-pubsub/v2/kafka"
	ratelimit "github.com/velonetics/velonetics-ratelimit/v3"
	"github.com/velonetics/lura/v2/logging"
	"github.com/velonetics/lura/v2/proxy"
)

const pipelineRetryDelay = time.Second

type consumerOptions struct {
	Name    string
	Topic   string
	Timeout time.Duration
	Workers int
	MaxRate float64
	Driver  kafkapkg.AsyncReaderConfig
}

func runConsumer(ctx context.Context, opts consumerOptions, logger logging.Logger, ping chan<- string, pingTicker *time.Ticker, next proxy.Proxy) error {
	reader, err := kafkapkg.NewAsyncReader(opts.Driver, opts.Topic, "velonetics-async-"+opts.Name)
	if err != nil {
		return err
	}
	defer reader.Close()

	logger.Info(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] Starting the consumer", opts.Name))

	if cap(ping) < 1 {
		logger.Warning(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] Ping channel with 0 capacity might block this async agent", opts.Name))
	}
	sendPing(ctx, ping, opts.Name)

	if opts.Workers < 1 {
		logger.Error(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] With less than 1 worker this agent does no work", opts.Name))
	}
	if opts.Workers > 1 {
		logger.Warning(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] Workers > 1 ignored; Kafka async processes messages sequentially to preserve offset commit order", opts.Name))
	}
	shouldProcess := newProcessor(ctx, opts, logger, next)
	var shouldExit atomic.Value
	shouldExit.Store(false)
	defer pingTicker.Stop()

	waitIfRequired := func() {}
	if opts.MaxRate > 0 {
		capacity := uint64(opts.MaxRate)
		if capacity == 0 {
			capacity = 1
		}
		bucket := ratelimit.NewTokenBucket(opts.MaxRate, capacity)
		pollingTime := time.Nanosecond * time.Duration(1e9/opts.MaxRate)
		waitIfRequired = func() {
			for !bucket.Allow() {
				time.Sleep(pollingTime)
			}
		}
	}

	var pending *kafka.Message

recvLoop:
	for !shouldExit.Load().(bool) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pingTicker.C:
			sendPing(ctx, ping, opts.Name)
			continue
		default:
		}

		var msg kafka.Message
		if pending != nil {
			msg = *pending
		} else {
			var fetchErr error
			msg, fetchErr = reader.FetchMessage(ctx)
			if fetchErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				logger.Error(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] FetchMessage:", opts.Name), fetchErr)
				shouldExit.Store(true)
				break recvLoop
			}
		}

		waitIfRequired()

		if shouldProcess(msg) {
			if err := reader.CommitMessages(ctx, msg); err != nil {
				logger.Error(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] CommitMessages:", opts.Name), err)
				shouldExit.Store(true)
				break recvLoop
			}
			pending = nil
			continue
		}

		pending = &msg
		logger.Warning(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] backend pipeline failed; retrying offset %d", opts.Name, msg.Offset))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pipelineRetryDelay):
		}
	}

	logger.Warning(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] Consumer stopped", opts.Name))
	return nil
}

func sendPing(ctx context.Context, ping chan<- string, name string) {
	select {
	case ping <- name:
	case <-ctx.Done():
	default:
	}
}

func newProcessor(ctx context.Context, opts consumerOptions, logger logging.Logger, next proxy.Proxy) func(kafka.Message) bool {
	return func(msg kafka.Message) bool {
		headers := map[string][]string{}
		if opts.Driver.KeyMeta != "" && len(msg.Key) > 0 {
			headers[opts.Driver.KeyMeta] = []string{string(msg.Key)}
		}

		req := proxy.Request{
			Params:  map[string]string{},
			Headers: headers,
			Body:    io.NopCloser(bytes.NewBuffer(msg.Value)),
		}
		contxt, cancel := context.WithTimeout(ctx, opts.Timeout)
		defer cancel()

		_, err := next(contxt, &req)
		if err != nil {
			logger.Error(fmt.Sprintf("[SERVICE: AsyncAgent][Kafka][%s] proxy pipe:", opts.Name), err)
			return false
		}
		return true
	}
}
