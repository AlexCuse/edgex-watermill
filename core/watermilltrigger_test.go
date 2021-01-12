package core

import (
	"bytes"
	"errors"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/appcontext"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/logger"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOutput_NilOutputData(t *testing.T) {
	sut := watermillTrigger{}

	err := sut.output("", &appcontext.Context{})

	require.NoError(t, err)
}

func TestOutput_NilPub(t *testing.T) {
	sut := watermillTrigger{}

	err := sut.output("", &appcontext.Context{
		OutputData: []byte("OK"),
	})

	require.NoError(t, err)
}

func TestOutput_MarshalError(t *testing.T) {
	topic := uuid.New().String()

	ctx := appcontext.Context{
		LoggingClient:       logger.MockLogger{},
		OutputData:          []byte("OK"),
		CorrelationID:       uuid.New().String(),
		ResponseContentType: uuid.New().String(),
	}

	msg := message.NewMessage("", []byte{})

	marshaler := mockMarshaler{}

	marshaler.On("Execute", mock.MatchedBy(func(envelope types.MessageEnvelope) bool {
		return ctx.CorrelationID == envelope.CorrelationID && ctx.ResponseContentType == envelope.ContentType && bytes.Equal(ctx.OutputData, envelope.Payload)
	})).Return(msg, errors.New(""))

	sut := watermillTrigger{marshaler: marshaler.Execute, pub: &mockPublisher{}}

	err := sut.output(topic, &ctx)

	require.Error(t, err)
}

func TestOutput_PublishError(t *testing.T) {
	topic := uuid.New().String()

	ctx := appcontext.Context{
		LoggingClient:       logger.MockLogger{},
		OutputData:          []byte("OK"),
		CorrelationID:       uuid.New().String(),
		ResponseContentType: uuid.New().String(),
	}

	msg := message.NewMessage(ctx.CorrelationID, ctx.OutputData)

	marshaler := mockMarshaler{}

	marshaler.On("Execute", mock.MatchedBy(func(envelope types.MessageEnvelope) bool {
		return ctx.CorrelationID == envelope.CorrelationID && ctx.ResponseContentType == envelope.ContentType && bytes.Equal(ctx.OutputData, envelope.Payload)
	})).Return(msg, nil)

	pub := mockPublisher{}
	pub.On("Publish", topic, msg).Return(errors.New(""))

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Execute}

	err := sut.output(topic, &ctx)

	require.Error(t, err)
}

func TestOutput(t *testing.T) {
	topic := uuid.New().String()

	ctx := appcontext.Context{
		LoggingClient:       logger.MockLogger{},
		OutputData:          []byte("OK"),
		CorrelationID:       uuid.New().String(),
		ResponseContentType: uuid.New().String(),
	}

	marshaled := message.Message{}

	pub := mockPublisher{}
	pub.On("Publish", topic, &marshaled).Return(nil)

	marshaler := mockMarshaler{}
	marshaler.On("Execute", mock.MatchedBy(func(envelope types.MessageEnvelope) bool {
		return ctx.CorrelationID == envelope.CorrelationID && ctx.ResponseContentType == envelope.ContentType && bytes.Equal(ctx.OutputData, envelope.Payload)
	})).Return(&marshaled, nil)

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Execute}

	err := sut.output(topic, &ctx)

	require.NoError(t, err)

	require.Equal(t, 1, len(marshaler.Calls))
	require.Equal(t, ctx.CorrelationID, marshaler.Calls[0].Arguments[0].(types.MessageEnvelope).CorrelationID)
	require.Equal(t, ctx.OutputData, marshaler.Calls[0].Arguments[0].(types.MessageEnvelope).Payload)
	require.Equal(t, ctx.ResponseContentType, marshaler.Calls[0].Arguments[0].(types.MessageEnvelope).ContentType)

	require.Equal(t, 1, len(pub.Calls))
	require.Equal(t, topic, pub.Calls[0].Arguments[0])
	msg, ok := pub.Calls[0].Arguments[1].(*message.Message)
	require.True(t, ok)
	require.Equal(t, &marshaled, msg)
}

func TestBackground_MarshalError(t *testing.T) {
	topic := uuid.New().String()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.New().String(),
		ContentType:   uuid.New().String(),
	}

	marshaler := mockMarshaler{}

	marshaler.On("Execute", env).Return(nil, errors.New(""))

	sut := watermillTrigger{marshaler: marshaler.Execute}

	err := sut.background(topic, env)

	require.Error(t, err)
}

func TestBackground_PublishError(t *testing.T) {
	topic := uuid.New().String()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.New().String(),
		ContentType:   uuid.New().String(),
	}

	msg := message.NewMessage(env.CorrelationID, env.Payload)

	marshaler := mockMarshaler{}

	marshaler.On("Execute", env).Return(msg, nil)

	pub := mockPublisher{}
	pub.On("Publish", topic, msg).Return(errors.New(""))

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Execute}

	err := sut.background(topic, env)

	require.Error(t, err)
}

func TestBackground(t *testing.T) {
	topic := uuid.New().String()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.New().String(),
		ContentType:   uuid.New().String(),
	}

	marshaled := message.Message{}

	pub := mockPublisher{}
	pub.On("Publish", topic, &marshaled).Return(nil)

	marshaler := mockMarshaler{}
	marshaler.On("Execute", env).Return(&marshaled, nil)

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Execute}

	err := sut.background(topic, env)

	require.NoError(t, err)

	require.Equal(t, 1, len(pub.Calls))
	require.Equal(t, topic, pub.Calls[0].Arguments[0])
	msg, ok := pub.Calls[0].Arguments[1].(*message.Message)
	require.True(t, ok)
	require.Equal(t, &marshaled, msg)
}
