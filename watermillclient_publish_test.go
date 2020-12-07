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
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

type mockPublisher struct {
	calls []struct {
		topic   string
		payload []*message.Message
	}
	closed bool
}

func (mp *mockPublisher) Publish(topic string, messages ...*message.Message) error {
	mp.calls = append(mp.calls, struct {
		topic   string
		payload []*message.Message
	}{topic, messages})

	return nil
}

func (mp *mockPublisher) Close() error { return nil }

type mockMarshaler struct {
	marshaled *message.Message
}

func (m *mockMarshaler) Marshal(envelope types.MessageEnvelope) (*message.Message, error) {
	return m.marshaled, nil
}

func TestPublish(t *testing.T) {
	topic := uuid.New().String()
	payload := types.MessageEnvelope{}
	marshaled := &message.Message{}

	publisher := mockPublisher{}

	client, err := newWatermillClientWithOptions(context.Background(), &publisher, nil, WatermillClientOptions{
		Marshaler: &mockMarshaler{marshaled: marshaled},
	})

	require.Nil(t, err, "should initialize client")

	err = client.Publish(payload, topic)

	require.Nil(t, err, "publish should succeed")

	require.Equal(t, 1, len(publisher.calls), "should call watermill publish once")
	require.Equal(t, topic, publisher.calls[0].topic, "at the passed topic")
	require.Equal(t, marshaled, publisher.calls[0].payload[0], "with the marshaled envelope")
}
