package async

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkapkg "github.com/velonetics/velonetics-pubsub/v2/kafka"
	"github.com/velonetics/lura/v2/async"
	"github.com/velonetics/lura/v2/logging"
)

const minExecutionTime = 5 * time.Second

func StartAgent(ctx context.Context, opts async.Options) bool {
	if _, err := kafkapkg.ParseAsyncDriverConfig(opts.Agent.ExtraConfig); errors.Is(err, kafkapkg.ErrAsyncDriverNotFound) {
		return false
	}

	kafkaF := func(ctx context.Context, l logging.Logger) error {
		driver, err := kafkapkg.ParseAsyncDriverConfig(opts.Agent.ExtraConfig)
		if err != nil {
			return err
		}
		return runConsumer(ctx, consumerOptions{
			Name:    opts.Agent.Name,
			Topic:   opts.Agent.Consumer.Topic,
			Timeout: opts.Agent.Consumer.Timeout,
			Workers: opts.Agent.Consumer.Workers,
			MaxRate: opts.Agent.Consumer.MaxRate,
			Driver:  *driver,
		}, l, opts.AgentPing, time.NewTicker(opts.Agent.Connection.HealthInterval), opts.Proxy)
	}

	opts.G.Go(func() error {
		for i := 0; opts.ShouldContinue(i); i++ {
			delay := opts.BackoffF(i)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}

			start := time.Now()
			if err := kafkaF(ctx, opts.Logger); err != nil {
				opts.Logger.Error(fmt.Sprintf("[SERVICE: Asyncagent][%s] building the kafka consumer:", opts.Agent.Name), err)
			}
			if time.Since(start) > minExecutionTime {
				i = 0
			}
		}
		return errTooManyRetries
	})

	return true
}

var errTooManyRetries = errors.New("too many retries")
