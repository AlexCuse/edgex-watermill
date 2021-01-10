package core

import (
	"errors"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/appcontext"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/logger"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
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

	pub := mockPublisher{}

	marshaler := mockMarshaler{
		err: errors.New("mocked error"),
	}

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Marshal}

	err := sut.output(topic, &ctx)

	require.Error(t, err)
	require.Equal(t, marshaler.err, err)
}

func TestOutput_PublishError(t *testing.T) {
	topic := uuid.New().String()

	ctx := appcontext.Context{
		LoggingClient:       logger.MockLogger{},
		OutputData:          []byte("OK"),
		CorrelationID:       uuid.New().String(),
		ResponseContentType: uuid.New().String(),
	}

	pub := mockPublisher{
		err: errors.New("mocked error"),
	}

	marshaler := mockMarshaler{}

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Marshal}

	err := sut.output(topic, &ctx)

	require.Error(t, err)
	require.Equal(t, pub.err, err)

	require.NotNil(t, marshaler.lastCall)

	require.Equal(t, ctx.CorrelationID, marshaler.lastCall.CorrelationID)
	require.Equal(t, ctx.OutputData, marshaler.lastCall.Payload)
	require.Equal(t, ctx.ResponseContentType, marshaler.lastCall.ContentType)
}

func TestOutput(t *testing.T) {
	topic := uuid.New().String()

	ctx := appcontext.Context{
		LoggingClient:       logger.MockLogger{},
		OutputData:          []byte("OK"),
		CorrelationID:       uuid.New().String(),
		ResponseContentType: uuid.New().String(),
	}

	pub := mockPublisher{}

	marshaler := mockMarshaler{
		marshaled: &message.Message{},
	}

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Marshal}

	err := sut.output(topic, &ctx)

	require.NoError(t, err)

	require.NotNil(t, marshaler.lastCall)

	require.Equal(t, ctx.CorrelationID, marshaler.lastCall.CorrelationID)
	require.Equal(t, ctx.OutputData, marshaler.lastCall.Payload)
	require.Equal(t, ctx.ResponseContentType, marshaler.lastCall.ContentType)

	require.Equal(t, 1, len(pub.calls))
	require.Equal(t, topic, pub.calls[0].topic)

	require.Equal(t, 1, len(pub.calls[0].payload))
	msg := pub.calls[0].payload[0]
	require.Equal(t, msg, marshaler.marshaled)
}

func TestBackground_MarshalError(t *testing.T) {
	topic := uuid.New().String()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.New().String(),
		ContentType:   uuid.New().String(),
	}

	pub := mockPublisher{}

	marshaler := mockMarshaler{
		err: errors.New("mocked error"),
	}

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Marshal}

	err := sut.background(topic, env)

	require.Error(t, err)
	require.Equal(t, marshaler.err, err)
}

func TestBackground_PublishError(t *testing.T) {
	topic := uuid.New().String()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.New().String(),
		ContentType:   uuid.New().String(),
	}

	pub := mockPublisher{
		err: errors.New("mocked error"),
	}

	marshaler := mockMarshaler{}

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Marshal}

	err := sut.background(topic, env)

	require.Error(t, err)
	require.Equal(t, pub.err, err)

	require.NotNil(t, marshaler.lastCall)

	require.Equal(t, env, *marshaler.lastCall)
}

func TestBackground(t *testing.T) {
	topic := uuid.New().String()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.New().String(),
		ContentType:   uuid.New().String(),
	}

	pub := mockPublisher{}

	marshaler := mockMarshaler{
		marshaled: &message.Message{},
	}

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Marshal}

	err := sut.background(topic, env)

	require.NoError(t, err)

	require.NotNil(t, marshaler.lastCall)

	require.Equal(t, env, *marshaler.lastCall)

	require.Equal(t, 1, len(pub.calls))
	require.Equal(t, topic, pub.calls[0].topic)

	require.Equal(t, 1, len(pub.calls[0].payload))
	msg := pub.calls[0].payload[0]
	require.Equal(t, msg, marshaler.marshaled)
}
