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
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewWatermillClient(t *testing.T) {
	format := &RawMessageFormat{}

	client, err := NewWatermillClient(context.Background(), nil, nil, format)

	require.Nil(t, err, "should not return error")
	require.NotNil(t, client, "should return initialized client")

	require.NotNil(t, client.(*watermillClient).marshaler, "should assign Marshaler from format")
}

func TestNewWatermillClient_Default_EdgexJSON(t *testing.T) {
	client, err := NewWatermillClient(context.Background(), nil, nil, nil)

	require.Nil(t, err, "should not return error")
	require.NotNil(t, client, "should return initialized client")

	require.NotNil(t, client.(*watermillClient).marshaler, "should assign Marshaler from format")
}

func TestPublish(t *testing.T) {
	topic := uuid.New().String()
	payload := types.MessageEnvelope{}
	marshaled := &message.Message{}

	publisher := mockPublisher{}
	publisher.On("Publish", topic, marshaled).Return(nil)

	marshaler := mockMarshaler{}
	marshaler.On("Execute", payload).Return(marshaled, nil)

	client, err := newWatermillClientWithOptions(context.Background(), &publisher, nil, WatermillClientOptions{
		Marshaler: marshaler.Execute,
	})

	require.Nil(t, err, "should initialize client")

	err = client.Publish(payload, topic)

	require.Nil(t, err, "publish should succeed")

	require.Equal(t, 1, len(publisher.Calls), "should call watermill publish once")
	require.Equal(t, topic, publisher.Calls[0].Arguments[0], "at the passed topic")
	require.Equal(t, marshaled, publisher.Calls[0].Arguments[1], "with the marshaled envelope")
}

func TestSubscribe(t *testing.T) {
	msgs := make(chan *message.Message, 1)
	env := types.MessageEnvelope{}
	msg := message.NewMessage(uuid.New().String(), []byte("OK"))

	topicChannel := types.TopicChannel{
		Topic:    "test-topic",
		Messages: make(chan types.MessageEnvelope),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subscriber := mockSubscriber{}

	subscriber.On("Subscribe", ctx, topicChannel.Topic).Return(msgs, nil)

	unmarshaler := mockUnmarshaler{}

	unmarshaler.On("Execute", msg, mock.Anything).Return(env, nil)

	client, err := newWatermillClientWithOptions(ctx, nil, &subscriber, WatermillClientOptions{
		Unmarshaler: unmarshaler.Execute,
	})

	require.NoError(t, err, "should initialize client")

	err = client.Subscribe([]types.TopicChannel{topicChannel}, nil)

	require.Nil(t, err, "subscribe should succeed")

	go func(m chan *message.Message) {
		time.Sleep(1 * time.Millisecond)
		msgs <- msg
	}(msgs)

	receiveMessage := true
	var receivedMessage *types.MessageEnvelope

	for receiveMessage {
		select {
		case m := <-topicChannel.Messages:
			receivedMessage = &m
			receiveMessage = false
		case <-ctx.Done():
			receiveMessage = false
		}
	}

	require.Equal(t, &env, receivedMessage, "should receive unmarshaled result")
}
