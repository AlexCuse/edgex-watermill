package core

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/appcontext"
	"github.com/edgexfoundry/app-functions-sdk-go/appsdk"
	"github.com/edgexfoundry/go-mod-bootstrap/bootstrap"
	"github.com/edgexfoundry/go-mod-core-contracts/clients"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/logger"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"sync"
)

type watermillTrigger struct {
	pub              message.Publisher
	sub              message.Subscriber
	marshaler        WatermillMarshaler
	unmarshaler      WatermillUnmarshaler
	context          context.Context
	cancel           context.CancelFunc
	sdkTriggerConfig appsdk.TriggerConfig
}

func (trigger *watermillTrigger) input(watermillMessage *message.Message, logger logger.LoggingClient) {
	msg, err := trigger.unmarshaler(watermillMessage)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to unmarshal message: %s", err.Error()))
		watermillMessage.Nack()
		return
	}

	edgexContext := trigger.sdkTriggerConfig.ContextBuilder(msg)

	logger.Trace("Received message", "topic", edgexContext.Configuration.Binding.SubscribeTopic, clients.CorrelationHeader, edgexContext.CorrelationID)

	err = trigger.sdkTriggerConfig.MessageProcessor(edgexContext, msg)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to process message: %s", err.Error()))
		watermillMessage.Nack()
		return
	}

	err = trigger.output(trigger.sdkTriggerConfig.Config.Binding.PublishTopic, edgexContext)

	if err != nil {
		logger.Error(fmt.Sprintf("Trigger failed to publish output: %v", err))
		watermillMessage.Nack() // if it was processed but not published ack might be appropriate?
		return
	}

	watermillMessage.Ack()
}

func (trigger *watermillTrigger) output(publishTopic string, ctx *appcontext.Context) error {
	if ctx.OutputData != nil && trigger.pub != nil {
		msg, err := trigger.marshaler(types.MessageEnvelope{
			CorrelationID: ctx.CorrelationID,
			Payload:       ctx.OutputData,
			ContentType:   ctx.ResponseContentType,
		})

		if err != nil {
			return err
		}

		err = trigger.pub.Publish(publishTopic, msg)

		if err != nil {
			return err
		}

		ctx.LoggingClient.Trace("Published message to trigger output", "topic", publishTopic, clients.CorrelationHeader, ctx.CorrelationID)
	}
	return nil
}

func (trigger *watermillTrigger) background(publishTopic string, bg types.MessageEnvelope) error {
	msg, err := trigger.marshaler(bg)

	if err != nil {
		return err
	}

	err = trigger.pub.Publish(publishTopic, msg)

	if err != nil {
		return err
	}

	return nil
}

func (trigger *watermillTrigger) Initialize(wg *sync.WaitGroup, ctx context.Context, background <-chan types.MessageEnvelope) (bootstrap.Deferred, error) {
	logger := trigger.sdkTriggerConfig.Logger
	cfg := trigger.sdkTriggerConfig.Config

	logger.Info(fmt.Sprintf("Initializing trigger for '%s'", cfg.MessageBus.Type))

	logger.Info(fmt.Sprintf("Subscribing to topic: '%s' @ %s://%s:%d",
		cfg.Binding.SubscribeTopic,
		cfg.MessageBus.SubscribeHost.Protocol,
		cfg.MessageBus.SubscribeHost.Host,
		cfg.MessageBus.SubscribeHost.Port))

	msgs, err := trigger.sub.Subscribe(trigger.context, cfg.Binding.SubscribeTopic)

	if err != nil {
		return nil, err
	}

	receiveMessage := true

	if len(cfg.MessageBus.PublishHost.Host) > 0 {
		logger.Info(fmt.Sprintf("Publishing to topic: '%s' @ %s://%s:%d",
			cfg.Binding.PublishTopic,
			cfg.MessageBus.PublishHost.Protocol,
			cfg.MessageBus.PublishHost.Host,
			cfg.MessageBus.PublishHost.Port))
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		for receiveMessage {
			select {
			case <-trigger.context.Done():
				return

			case input := <-msgs:
				go func() {
					trigger.input(input, logger)
				}()

			case bg := <-background:
				go func() {
					trigger.background(trigger.sdkTriggerConfig.Config.Binding.PublishTopic, bg)
				}()

			}
		}
	}()

	deferred := func() {
		logger.Info("Disconnecting trigger")
		if trigger.sub != nil {
			err := trigger.sub.Close()
			if err != nil {
				logger.Error("Unable to disconnect trigger subscriber", "error", err.Error())
			}
		}

		if trigger.pub != nil {
			err = trigger.pub.Close()
			if err != nil {
				logger.Error("Unable to disconnect trigger publisher", "error", err.Error())
			}
		}
	}
	return deferred, nil
}

func NewWatermillTrigger(publisher message.Publisher, subscriber message.Subscriber, format MessageFormat, ctx context.Context, tc appsdk.TriggerConfig) appsdk.Trigger {
	ctx, cancel := context.WithCancel(ctx)
	return &watermillTrigger{
		pub:              publisher,
		sub:              subscriber,
		context:          ctx,
		cancel:           cancel,
		sdkTriggerConfig: tc,
		marshaler:        format.marshal,
		unmarshaler:      format.unmarshal,
	}
}

func (trigger *watermillTrigger) Stop() {
	if trigger.cancel != nil {
		trigger.cancel()
	}
}
