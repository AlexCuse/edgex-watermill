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

package core

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/hashicorp/go-multierror"
)

type watermillClient struct {
	pub         message.Publisher
	sub         message.Subscriber
	context     context.Context
	marshaler   WatermillMarshaler
	unmarshaler WatermillUnmarshaler
	decryptor   binaryModifier
	encryptor   binaryModifier
}

const (
	EdgeXContentType = "edgex_content_type"
	EdgeXChecksum    = "edgex_checksum"
)

func (c *watermillClient) Connect() error {
	return nil
}

func (c *watermillClient) Publish(env types.MessageEnvelope, topic string) error {
	m, err := c.marshaler(env, c.encryptor)

	if err != nil {
		return err
	}
	err = c.pub.Publish(topic, m)

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
				errors <- err
				return
			}
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-sub:
					formattedMessage, err := c.unmarshaler(msg, c.decryptor)

					if err != nil {
						//TODO: can we get message errors from watermill Subscriber as well?  May need to wire in differently
						errors <- err
					} else {
						topic.Messages <- formattedMessage
						msg.Ack() //TODO: explore options for different ack/nack behavior on pipeline completion?
					}
				}
			}
		}(c.context, c.sub, topic, messageErrors)
	}

	return nil
}

func (c *watermillClient) Disconnect() error {
	var result error

	result = multierror.Append(result, c.pub.Close())
	result = multierror.Append(result, c.sub.Close())

	return result
}

func NewWatermillClient(ctx context.Context, pub message.Publisher, sub message.Subscriber, format WireFormat, watermillConfig *WatermillConfig) (messaging.MessageClient, error) {
	if format == nil {
		format = &EdgeXWireFormat{}
	}

	return newWatermillClientWithOptions(ctx, pub, sub, WatermillClientOptions{
		Marshaler:   format.marshal,
		Unmarshaler: format.unmarshal,
	}, watermillConfig)
}

type WatermillClientOptions struct {
	Marshaler   WatermillMarshaler
	Unmarshaler WatermillUnmarshaler
}

func newWatermillClientWithOptions(ctx context.Context, pub message.Publisher, sub message.Subscriber, opt WatermillClientOptions, config *WatermillConfig) (messaging.MessageClient, error) {
	client := &watermillClient{
		pub:         pub,
		sub:         sub,
		context:     ctx,
		marshaler:   opt.Marshaler,
		unmarshaler: opt.Unmarshaler,
		encryptor:   noopModifier,
		decryptor:   noopModifier,
	}

	var err error

	if config != nil {
		protection, err := newAESProtection(config)

		if err == nil && protection != nil { // else err is going to be returned
			client.encryptor = protection.encrypt
			client.decryptor = protection.decrypt
		}
	}

	return client, err
}
