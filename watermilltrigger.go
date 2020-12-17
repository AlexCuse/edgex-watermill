package edgex_watermill

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/edgexfoundry/app-functions-sdk-go/appsdk"
	"github.com/edgexfoundry/go-mod-bootstrap/bootstrap"
	"github.com/edgexfoundry/go-mod-core-contracts/clients"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
	"sync"
)

type watermillTrigger struct {
	pub              message.Publisher
	sub              message.Subscriber
	context          context.Context
	contextBuilder   appsdk.TriggerContextBuilder
	messageProcessor appsdk.TriggerMessageProcessor
}

func (trigger *watermillTrigger) Initialize(wg *sync.WaitGroup, ctx context.Context, background <-chan types.MessageEnvelope) (bootstrap.Deferred, error) {
	var err error
	//TODO: put sdk logger on trigger/client, figure out how to adapt watermill logger
	fakeContext := trigger.contextBuilder(types.MessageEnvelope{})

	logger := fakeContext.LoggingClient

	logger.Info(fmt.Sprintf("Initializing trigger for '%s'", fakeContext.Configuration.MessageBus.Type))

	logger.Info(fmt.Sprintf("Subscribing to topic: '%s' @ %s://%s:%d",
		fakeContext.Configuration.Binding.SubscribeTopic,
		fakeContext.Configuration.MessageBus.SubscribeHost.Protocol,
		fakeContext.Configuration.MessageBus.SubscribeHost.Host,
		fakeContext.Configuration.MessageBus.SubscribeHost.Port))

	msgs, err := trigger.sub.Subscribe(trigger.context, fakeContext.Configuration.Binding.SubscribeTopic)

	if err != nil {
		return nil, err
	}

	receiveMessage := true

	if len(fakeContext.Configuration.MessageBus.PublishHost.Host) > 0 {
		logger.Info(fmt.Sprintf("Publishing to topic: '%s' @ %s://%s:%d",
			fakeContext.Configuration.Binding.PublishTopic,
			fakeContext.Configuration.MessageBus.PublishHost.Protocol,
			fakeContext.Configuration.MessageBus.PublishHost.Host,
			fakeContext.Configuration.MessageBus.PublishHost.Port))
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

					logger.Trace("Received message", "topic", fakeContext.Configuration.Binding.SubscribeTopic, clients.CorrelationHeader, correlationID)

					contentType := input.Metadata.Get(EdgeXContentType)

					if contentType == "" {
						contentType = clients.ContentTypeCBOR

						if input.Payload[0] == byte('{') || input.Payload[0] == byte('[') {
							contentType = clients.ContentTypeJSON
						}
					}

					msg := types.MessageEnvelope{
						CorrelationID: correlationID,
						Payload:       input.Payload,
						ContentType:   contentType,
					}

					edgexContext := trigger.contextBuilder(msg)

					err := trigger.messageProcessor(edgexContext, msg)

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

						err := trigger.pub.Publish(fakeContext.Configuration.Binding.PublishTopic, msg)
						if err != nil {
							logger.Error(fmt.Sprintf("Trigger failed to publish output: %v", err))
							input.Nack() // if it was processed but not published ack might be appropriate?
							return
						}

						logger.Trace("Published message to trigger output", "topic", fakeContext.Configuration.Binding.PublishTopic, clients.CorrelationHeader, correlationID)
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

func NewWatermillTrigger(publisher message.Publisher, subscriber message.Subscriber, ctx context.Context, contextFunc appsdk.TriggerContextBuilder, messageFunc appsdk.TriggerMessageProcessor) appsdk.Trigger {
	return &watermillTrigger{
		pub:              publisher,
		sub:              subscriber,
		context:          ctx,
		contextBuilder:   contextFunc,
		messageProcessor: messageFunc,
	}
}
