package core

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/appcontext"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/appsdk"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/util"
	"github.com/edgexfoundry/go-mod-bootstrap/v2/bootstrap"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/clients"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"strings"
	"sync"
)

type watermillTrigger struct {
	pub              message.Publisher
	sub              message.Subscriber
	marshaler        WatermillMarshaler
	unmarshaler      WatermillUnmarshaler
	topics           []string
	context          context.Context
	cancel           context.CancelFunc
	sdkTriggerConfig appsdk.TriggerConfig
}

func (trigger *watermillTrigger) input(watermillMessage *message.Message, receiveTopic string, publishTopic string) {
	logger := trigger.sdkTriggerConfig.Logger

	msg, err := trigger.unmarshaler(watermillMessage)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to unmarshal message: %s", err.Error()))
		watermillMessage.Nack()
		return
	}

	edgexContext := trigger.sdkTriggerConfig.ContextBuilder(msg)

	logger.Trace("Received message", "topic", receiveTopic, clients.CorrelationHeader, edgexContext.CorrelationID)

	err = trigger.sdkTriggerConfig.MessageProcessor(edgexContext, msg)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to process message: %s", err.Error()))
		watermillMessage.Nack()
		return
	}

	err = trigger.output(publishTopic, edgexContext)

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

	trigger.context, trigger.cancel = context.WithCancel(ctx)

	logger.Info(fmt.Sprintf("Initializing trigger for '%s'", cfg.MessageBus.Type))

	logger.Info(fmt.Sprintf("Subscribing to topic: '%s' @ %s://%s:%d",
		cfg.Binding.SubscribeTopics,
		cfg.MessageBus.SubscribeHost.Protocol,
		cfg.MessageBus.SubscribeHost.Host,
		cfg.MessageBus.SubscribeHost.Port))

	if len(strings.TrimSpace(trigger.sdkTriggerConfig.Config.Binding.SubscribeTopics)) == 0 {
		// Still allows subscribing to blank topic to receive all messages
		trigger.topics = append(trigger.topics, trigger.sdkTriggerConfig.Config.Binding.SubscribeTopics)
	} else {
		topics := util.DeleteEmptyAndTrim(strings.FieldsFunc(trigger.sdkTriggerConfig.Config.Binding.SubscribeTopics, util.SplitComma))
		for _, topic := range topics {
			trigger.topics = append(trigger.topics, topic)
		}
	}

	for _, topic := range trigger.topics {
		tributary, err := trigger.sub.Subscribe(trigger.context, topic)

		if err != nil {
			return nil, err
		}

		wg.Add(1)

		go func(waitgroup *sync.WaitGroup, collectFrom <-chan *message.Message, t string) {
			defer waitgroup.Done()
			for {
				select {
				case <-trigger.context.Done():
					return

				case m := <-collectFrom:
					go func() {
						trigger.input(m, t, trigger.sdkTriggerConfig.Config.Binding.PublishTopic)
					}()

				}
			}
		}(wg, tributary, topic)
	}

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

		for {
			select {
			case <-trigger.context.Done():
				return

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
				logger.Error("Unable to disconnect trigger Subscriber", "error", err.Error())
			}
		}

		if trigger.pub != nil {
			err := trigger.pub.Close()
			if err != nil {
				logger.Error("Unable to disconnect trigger Publisher", "error", err.Error())
			}
		}
	}

	return deferred, nil
}

func NewWatermillTrigger(publisher message.Publisher, subscriber message.Subscriber, format MessageFormat, tc appsdk.TriggerConfig) appsdk.Trigger {
	return &watermillTrigger{
		pub:              publisher,
		sub:              subscriber,
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
