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
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	msgs := make(chan *message.Message, 1)
	env := types.MessageEnvelope{}

	topicChannel := types.TopicChannel{
		Topic:    "test-topic",
		Messages: make(chan types.MessageEnvelope),
	}
	subscriber := mockSubscriber{
		subscriptionChannel: msgs,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := newWatermillClientWithOptions(ctx, nil, &subscriber, WatermillClientOptions{
		Unmarshaler: (&mockUnmarshaler{unmarshaled: env}).Unmarshal,
	})

	require.Nil(t, err, "should initialize client")

	err = client.Subscribe([]types.TopicChannel{topicChannel}, nil)

	require.Nil(t, err, "subscribe should succeed")

	go func(m chan *message.Message) {
		time.Sleep(1 * time.Millisecond)
		msgs <- message.NewMessage(uuid.New().String(), []byte("OK"))
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
