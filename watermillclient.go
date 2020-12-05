//
// Copyright (c) 2020 Alex Ullrich
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package edgex_watermill

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/go-mod-core-contracts/clients"
	"github.com/edgexfoundry/go-mod-messaging/messaging"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
)

type watermillClient struct {
	message.Publisher
	message.Subscriber
	context   context.Context
	marshal   WatermillMarshaler
	unmarshal WatermillUnmarshaler
}

const (
	EdgeXContentType = "edgex-content-type"
	EdgeXChecksum    = "edgex-checksum"
)

func (c *watermillClient) Connect() error {
	return nil
}

func (c *watermillClient) Publish(env types.MessageEnvelope, topic string) error {
	m, err := c.marshal(env)

	if err != nil {
		return err
	}
	err = c.Publisher.Publish(topic, m)

	if err != nil {
		return err
	}

	return nil
}

func (c *watermillClient) Subscribe(topics []types.TopicChannel, messageErrors chan error) error {
	for _, topic := range topics {
		go func(ctx context.Context, s message.Subscriber, topic types.TopicChannel, errors chan error) {
			sub, err := s.Subscribe(ctx, topic.Topic)

			if err != nil {
				panic(err)
			}
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-sub:
					formattedMessage, err := c.unmarshal(msg)

					if err != nil {
						//TODO: can we get message errors from watermill subscriber as well?  May need to wire in differently
						errors <- err
					} else {
						topic.Messages <- formattedMessage
						msg.Ack() //TODO: explore options for different ack/nack behavior on pipeline completion?
					}
				}
			}
		}(c.context, c.Subscriber, topic, messageErrors)
	}

	return nil
}

func (c *watermillClient) Disconnect() error {
	c.Publisher.Close()
	c.Subscriber.Close()
	return nil
}

func NewWatermillClient(ctx context.Context, pub message.Publisher, sub message.Subscriber) (messaging.MessageClient, error) {
	client := watermillClient{
		pub,
		sub,
		ctx,
		defaultMarshaler,
		defaultUnmarshaler,
	}

	return &client, nil
}

type WatermillClientOptions struct {
	Marshaler   WatermillMarshaler
	Unmarshaler WatermillUnmarshaler
}

func NewWatermillClientWithOptions(ctx context.Context, pub message.Publisher, sub message.Subscriber, opt WatermillClientOptions) (messaging.MessageClient, error) {
	client, err := NewWatermillClient(ctx, pub, sub)

	if err != nil {
		return client, err
	}

	if opt.Marshaler != nil {
		client.(*watermillClient).marshal = opt.Marshaler
	}

	if opt.Unmarshaler != nil {
		client.(*watermillClient).unmarshal = opt.Unmarshaler
	}

	return client, nil
}

type WatermillUnmarshaler func(*message.Message) (types.MessageEnvelope, error)

func defaultUnmarshaler(msg *message.Message) (types.MessageEnvelope, error) {
	correlationID := msg.UUID

	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	checksum := msg.Metadata.Get(EdgeXChecksum)

	contentType := msg.Metadata.Get(EdgeXContentType)

	if contentType == "" {
		if msg.Payload[0] == byte('{') || msg.Payload[0] == byte('[') {
			contentType = clients.ContentTypeJSON
		} else {
			contentType = clients.ContentTypeCBOR
		}
	}

	formattedMessage := types.MessageEnvelope{
		Payload:       msg.Payload,
		CorrelationID: correlationID,
		ContentType:   contentType,
		Checksum:      checksum,
	}
	return formattedMessage, nil
}

type WatermillMarshaler func (types.MessageEnvelope) (*message.Message, error)

func defaultMarshaler (envelope types.MessageEnvelope) (*message.Message, error){
	m := message.NewMessage(envelope.CorrelationID, envelope.Payload)

	m.Metadata.Set(EdgeXChecksum, envelope.Checksum)
	m.Metadata.Set(EdgeXContentType, envelope.ContentType)

	return m, nil
}
