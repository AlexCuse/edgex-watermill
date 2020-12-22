package core

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/appsdk"
	"github.com/edgexfoundry/go-mod-bootstrap/bootstrap"
	"github.com/edgexfoundry/go-mod-core-contracts/clients"
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
					msg, err := trigger.unmarshaler(input)

					if err != nil {
						logger.Error(fmt.Sprintf("Failed to unmarshal message: %s", err.Error()))
						input.Nack()
						return
					}

					edgexContext := trigger.sdkTriggerConfig.ContextBuilder(msg)

					logger.Trace("Received message", "topic", edgexContext.Configuration.Binding.SubscribeTopic, clients.CorrelationHeader, edgexContext.CorrelationID)

					err = trigger.sdkTriggerConfig.MessageProcessor(edgexContext, msg)

					if err != nil {
						logger.Error(fmt.Sprintf("Failed to process message: %s", err.Error()))
						input.Nack()
						return
					}

					if edgexContext.OutputData != nil {
						msg, err := trigger.marshaler(types.MessageEnvelope{
							CorrelationID: edgexContext.CorrelationID,
							Payload:       edgexContext.OutputData,
							ContentType:   edgexContext.ResponseContentType,
						})

						if err != nil {
							logger.Error(fmt.Sprintf("Trigger failed to prepare output for publish: %v", err))
							input.Nack() // if it was processed but not published ack might be appropriate?
							return
						}

						err = trigger.pub.Publish(edgexContext.Configuration.Binding.PublishTopic, msg)

						if err != nil {
							logger.Error(fmt.Sprintf("Trigger failed to publish output: %v", err))
							input.Nack() // if it was processed but not published ack might be appropriate?
							return
						}

						logger.Trace("Published message to trigger output", "topic", edgexContext.Configuration.Binding.PublishTopic, clients.CorrelationHeader, edgexContext.CorrelationID)
					}

					input.Ack()
				}()
				//TODO:
				/*
					case bg := <-background:
						go func() {
							err := trigger.Publish(bg, trigger.configuration.Binding.PublishTopic)
							if err != nil {
								logger.Error(fmt.Sprintf("Failed to publish background Message to bus, %v", err))
								return
							}

							logger.Trace("Published background message to bus", "topic", trigger.Configuration.Binding.PublishTopic, clients.CorrelationHeader, bg.CorrelationID)
						}()
				*/
			}
		}
	}()

	deferred := func() {
		logger.Info("Disconnecting trigger")
		err := trigger.sub.Close()
		if err != nil {
			logger.Error("Unable to disconnect trigger subscriber", "error", err.Error())
		}
		err = trigger.pub.Close()
		if err != nil {
			logger.Error("Unable to disconnect trigger publisher", "error", err.Error())
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
