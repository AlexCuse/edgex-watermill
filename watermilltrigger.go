package edgex_watermill

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/edgexfoundry/app-functions-sdk-go/appcontext"
	"github.com/edgexfoundry/app-functions-sdk-go/appsdk"
	"github.com/edgexfoundry/go-mod-bootstrap/bootstrap"
	"github.com/edgexfoundry/go-mod-core-contracts/clients"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
	"sync"
)

type watermillTrigger struct {
	message.Publisher
	message.Subscriber
	context            context.Context
	appContextSeed     appsdk.TriggerContextSeed
	processMessageFunc appsdk.ProcessMessageFunc
}

func (trigger *watermillTrigger) Initialize(wg *sync.WaitGroup, ctx context.Context, background <-chan types.MessageEnvelope) (bootstrap.Deferred, error) {
	var err error
	logger := trigger.appContextSeed.LoggingClient

	logger.Info(fmt.Sprintf("Initializing trigger for '%s'", trigger.appContextSeed.Configuration.MessageBus.Type))

	logger.Info(fmt.Sprintf("Subscribing to topic: '%s' @ %s://%s:%d",
		trigger.appContextSeed.Configuration.Binding.SubscribeTopic,
		trigger.appContextSeed.Configuration.MessageBus.SubscribeHost.Protocol,
		trigger.appContextSeed.Configuration.MessageBus.SubscribeHost.Host,
		trigger.appContextSeed.Configuration.MessageBus.SubscribeHost.Port))

	msgs, err := trigger.Subscriber.Subscribe(trigger.context, trigger.appContextSeed.Configuration.Binding.SubscribeTopic)

	if err != nil {
		return nil, err
	}

	receiveMessage := true

	if len(trigger.appContextSeed.Configuration.MessageBus.PublishHost.Host) > 0 {
		logger.Info(fmt.Sprintf("Publishing to topic: '%s' @ %s://%s:%d",
			trigger.appContextSeed.Configuration.Binding.PublishTopic,
			trigger.appContextSeed.Configuration.MessageBus.PublishHost.Protocol,
			trigger.appContextSeed.Configuration.MessageBus.PublishHost.Host,
			trigger.appContextSeed.Configuration.MessageBus.PublishHost.Port))
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
					correlationID := input.UUID

					if correlationID == "" {
						correlationID = uuid.New().String()
					}

					logger.Trace("Received message", "topic", trigger.appContextSeed.Configuration.Binding.SubscribeTopic, clients.CorrelationHeader, correlationID)

					contentType := input.Metadata.Get(EdgeXContentType)

					if contentType == "" {
						contentType = clients.ContentTypeCBOR

						if input.Payload[0] == byte('{') || input.Payload[0] == byte('[') {
							contentType = clients.ContentTypeJSON
						}
					}

					edgexContext := &appcontext.Context{
						CorrelationID:         correlationID,
						Configuration:         trigger.appContextSeed.Configuration,
						LoggingClient:         trigger.appContextSeed.LoggingClient,
						EventClient:           trigger.appContextSeed.EventClient,
						ValueDescriptorClient: trigger.appContextSeed.ValueDescriptorClient,
						CommandClient:         trigger.appContextSeed.CommandClient,
						NotificationsClient:   trigger.appContextSeed.NotificationsClient,
					}

					msg := types.MessageEnvelope{
						CorrelationID: correlationID,
						Payload:       input.Payload,
						ContentType:   contentType,
					}

					err := trigger.processMessageFunc(edgexContext, msg)

					if err != nil {
						input.Nack()
						return
					}

					if edgexContext.OutputData != nil {
						msg := message.NewMessage(edgexContext.CorrelationID, edgexContext.OutputData)
						msg.Metadata.Set(middleware.CorrelationIDMetadataKey, correlationID)
						msg.Metadata.Set(EdgeXChecksum, edgexContext.EventChecksum)

						if edgexContext.ResponseContentType != "" {
							msg.Metadata.Set(EdgeXContentType, edgexContext.ResponseContentType)
						} else {
							msg.Metadata.Set(EdgeXContentType, contentType)
						}

						err := trigger.Publish(trigger.appContextSeed.Configuration.Binding.PublishTopic, msg)
						if err != nil {
							logger.Error(fmt.Sprintf("Trigger failed to publish output: %v", err))
							input.Nack() // if it was processed but not published ack might be appropriate?
							return
						}

						logger.Trace("Published message to trigger output", "topic", trigger.appContextSeed.Configuration.Binding.PublishTopic, clients.CorrelationHeader, correlationID)
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
		err := trigger.Subscriber.Close()
		if err != nil {
			logger.Error("Unable to disconnect trigger subscriber", "error", err.Error())
		}
		err = trigger.Publisher.Close()
		if err != nil {
			logger.Error("Unable to disconnect trigger publisher", "error", err.Error())
		}
	}
	return deferred, nil
}

func NewWatermillTrigger(publisher message.Publisher, subscriber message.Subscriber, ctx context.Context, seed appsdk.TriggerContextSeed, messageFunc appsdk.ProcessMessageFunc) appsdk.Trigger {
	return &watermillTrigger{
		Publisher:          publisher,
		Subscriber:         subscriber,
		context:            ctx,
		appContextSeed:     seed,
		processMessageFunc: messageFunc,
	}
}
