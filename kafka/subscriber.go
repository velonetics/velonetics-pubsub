package kafka

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/velonetics/lura/v2/config"
	"github.com/velonetics/lura/v2/logging"
	"github.com/velonetics/lura/v2/proxy"
)

func initSubscriber(
	ctx context.Context,
	logger logging.Logger,
	remote *config.Backend,
) (proxy.Proxy, error) {
	cfg, err := parseSubscriberConfig(remote)
	if err != nil {
		return proxy.NoopProxy, err
	}
	if err := validateSubscriber(cfg); err != nil {
		return proxy.NoopProxy, err
	}

	dialer, err := newDialer(cfg.Reader.Cluster)
	if err != nil {
		return proxy.NoopProxy, err
	}

	groupID := cfg.Reader.Group.resolvedID()
	if groupID == "" {
		groupID = "velonetics-pubsub"
	}

	readerCfg := kafka.ReaderConfig{
		Brokers:        cfg.Reader.Cluster.Brokers,
		GroupID:        groupID,
		Topic:          cfg.Reader.Topics[0],
		Dialer:         dialer,
		IsolationLevel: isolationLevel(cfg.Reader.Group.IsolationLevel),
		SessionTimeout: parseDuration(cfg.Reader.Group.SessionTimeout, 10*time.Second),
		HeartbeatInterval: parseDuration(cfg.Reader.Group.HeartbeatInterval, 3*time.Second),
	}

	reader := kafka.NewReader(readerCfg)
	logPrefix := fmt.Sprintf("[BACKEND: kafka://%s/%s][PubSub/Kafka]", cfg.Reader.Cluster.Brokers[0], cfg.Reader.Topics[0])
	logger.Debug(logPrefix, "Subscriber initialized successfully")

	go func() {
		<-ctx.Done()
		_ = reader.Close()
	}()

	ef := proxy.NewEntityFormatter(remote)

	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return nil, err
		}

		var data map[string]interface{}
		if err := remote.Decoder(bytes.NewBuffer(msg.Value), &data); err != nil && err != io.EOF {
			return nil, err
		}

		resp := proxy.Response{Data: data, IsComplete: true}
		if cfg.Reader.KeyMeta != "" && len(msg.Key) > 0 {
			resp.Metadata.Headers = map[string][]string{
				cfg.Reader.KeyMeta: {string(msg.Key)},
			}
		}
		resp = ef.Format(resp)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			return nil, err
		}
		return &resp, nil
	}, nil
}
