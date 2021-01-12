package core

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/appcontext"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
)

//Publisher

type mockPublisher struct {
	calls []struct {
		topic   string
		payload []*message.Message
	}
	closed bool
	err    error
}

func (mp *mockPublisher) Publish(topic string, messages ...*message.Message) error {
	mp.calls = append(mp.calls, struct {
		topic   string
		payload []*message.Message
	}{topic, messages})

	return mp.err
}

func (mp *mockPublisher) Close() error { return nil }

type mockMarshaler struct {
	marshaled *message.Message
	err       error
	lastCall  *types.MessageEnvelope
}

func (m *mockMarshaler) Marshal(envelope types.MessageEnvelope) (*message.Message, error) {
	m.lastCall = &envelope
	return m.marshaled, m.err
}

// Subscriber
type mockSubscriber struct {
	calls []struct {
		topic   string
		payload []*message.Message
	}
	closed              bool
	subscriptionChannel chan *message.Message
}

func (mp *mockSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return mp.subscriptionChannel, nil
}

func (mp *mockSubscriber) Close() error { return nil }

type mockUnmarshaler struct {
	unmarshaled types.MessageEnvelope
}

func (m *mockUnmarshaler) Unmarshal(message *message.Message) (types.MessageEnvelope, error) {
	return m.unmarshaled, nil
}

// Message Processor
type mockMessageProcessor struct {
	lastCall struct {
		ctx *appcontext.Context
		env types.MessageEnvelope
	}
	err error
}

func (mp *mockMessageProcessor) process(ctx *appcontext.Context, env types.MessageEnvelope) error {
	mp.lastCall = struct {
		ctx *appcontext.Context
		env types.MessageEnvelope
	}{ctx: ctx, env: env}

	return mp.err
}
