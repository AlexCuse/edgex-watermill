//
// Copyright (c) 2021 Alex Ullrich
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
	"bytes"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/v3/pkg"
	"github.com/edgexfoundry/app-functions-sdk-go/v3/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/clients/logger"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestOutput_NilOutputData(t *testing.T) {
	sut := watermillTrigger{}

	err := sut.output(pkg.NewAppFuncContextForTest(uuid.NewString(), logger.MockLogger{}), nil)

	require.NoError(t, err)
}

func TestOutput_NilPub(t *testing.T) {
	sut := watermillTrigger{}

	ctx := pkg.NewAppFuncContextForTest(uuid.NewString(), logger.MockLogger{})
	ctx.SetResponseData([]byte("OK"))

	err := sut.output(ctx, &interfaces.FunctionPipeline{})

	require.NoError(t, err)
}

func TestOutput_MarshalError(t *testing.T) {
	topic := uuid.NewString()

	ctx := pkg.NewAppFuncContextForTest(uuid.NewString(), logger.NewMockClient())
	ctx.SetResponseData([]byte{})

	msg := message.NewMessage("", []byte{})

	marshaler := mockMarshaler{}

	marshaler.On("Execute", mock.MatchedBy(func(envelope types.MessageEnvelope) bool {
		return ctx.CorrelationID() == envelope.CorrelationID && ctx.ResponseContentType() == envelope.ContentType && bytes.Equal(ctx.ResponseData(), envelope.Payload)
	}), mock.AnythingOfType("core.binaryModifier")).Return(msg, errors.New(""))

	sut := watermillTrigger{marshaler: marshaler.Execute, pub: &mockPublisher{}, watermillConfig: &WatermillConfigWrapper{WatermillTrigger: WatermillConfig{PublishTopic: topic}}}

	err := sut.output(ctx, &interfaces.FunctionPipeline{})

	require.Error(t, err)
}

func TestOutput_PublishError(t *testing.T) {
	topic := uuid.NewString()

	ctx := pkg.NewAppFuncContextForTest(uuid.NewString(), logger.MockLogger{})
	ctx.SetResponseData([]byte(uuid.NewString()))

	msg := message.NewMessage(ctx.CorrelationID(), ctx.ResponseData())

	marshaler := mockMarshaler{}

	marshaler.On("Execute", mock.MatchedBy(func(envelope types.MessageEnvelope) bool {
		return ctx.CorrelationID() == envelope.CorrelationID && ctx.ResponseContentType() == envelope.ContentType && bytes.Equal(ctx.ResponseData(), envelope.Payload)
	}), mock.AnythingOfType("core.binaryModifier")).Return(msg, nil)

	pub := mockPublisher{}
	pub.On("Publish", topic, msg).Return(errors.New(""))

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Execute, watermillConfig: &WatermillConfigWrapper{WatermillTrigger: WatermillConfig{PublishTopic: topic}}}

	err := sut.output(ctx, &interfaces.FunctionPipeline{})

	require.Error(t, err)
}

func TestOutput(t *testing.T) {
	topic := uuid.NewString()

	ctx := pkg.NewAppFuncContextForTest(uuid.NewString(), logger.MockLogger{})
	ctx.SetResponseData([]byte{})

	marshaled := message.Message{}

	pub := mockPublisher{}
	pub.On("Publish", topic, &marshaled).Return(nil)

	marshaler := mockMarshaler{}
	marshaler.On("Execute", mock.MatchedBy(func(envelope types.MessageEnvelope) bool {
		return ctx.CorrelationID() == envelope.CorrelationID && ctx.ResponseContentType() == envelope.ContentType && bytes.Equal(ctx.ResponseData(), envelope.Payload)
	}), mock.AnythingOfType("core.binaryModifier")).Return(&marshaled, nil)

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Execute, watermillConfig: &WatermillConfigWrapper{WatermillTrigger: WatermillConfig{PublishTopic: topic}}}

	err := sut.output(ctx, &interfaces.FunctionPipeline{})

	require.NoError(t, err, nil)

	require.Equal(t, 1, len(marshaler.Calls))
	require.Equal(t, ctx.CorrelationID(), marshaler.Calls[0].Arguments[0].(types.MessageEnvelope).CorrelationID)
	require.Equal(t, ctx.ResponseData(), marshaler.Calls[0].Arguments[0].(types.MessageEnvelope).Payload)
	require.Equal(t, ctx.ResponseContentType(), marshaler.Calls[0].Arguments[0].(types.MessageEnvelope).ContentType)

	require.Equal(t, 1, len(pub.Calls))
	require.Equal(t, topic, pub.Calls[0].Arguments[0])
	msg, ok := pub.Calls[0].Arguments[1].(*message.Message)
	require.True(t, ok)
	require.Equal(t, &marshaled, msg)
}

func TestBackground_MarshalError(t *testing.T) {
	topic := uuid.NewString()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.NewString(),
		ContentType:   uuid.NewString(),
	}

	marshaler := mockMarshaler{}

	marshaler.On("Execute", env, mock.AnythingOfType("core.binaryModifier")).Return(nil, errors.New(""))

	sut := watermillTrigger{marshaler: marshaler.Execute}

	err := sut.background(MockBackgroundMessage{env, topic})

	require.Error(t, err)
}

func TestBackground_PublishError(t *testing.T) {
	topic := uuid.NewString()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.NewString(),
		ContentType:   uuid.NewString(),
	}

	msg := message.NewMessage(env.CorrelationID, env.Payload)

	marshaler := mockMarshaler{}

	marshaler.On("Execute", env, mock.AnythingOfType("core.binaryModifier")).Return(msg, nil)

	pub := mockPublisher{}
	pub.On("Publish", topic, msg).Return(errors.New(""))

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Execute}

	err := sut.background(MockBackgroundMessage{env, topic})

	require.Error(t, err)
}

func TestBackground(t *testing.T) {
	topic := uuid.NewString()

	env := types.MessageEnvelope{
		Payload:       []byte("OK"),
		CorrelationID: uuid.NewString(),
		ContentType:   uuid.NewString(),
	}

	marshaled := message.Message{}

	pub := mockPublisher{}
	pub.On("Publish", topic, &marshaled).Return(nil)

	marshaler := mockMarshaler{}
	marshaler.On("Execute", env, mock.AnythingOfType("core.binaryModifier")).Return(&marshaled, nil)

	sut := watermillTrigger{pub: &pub, marshaler: marshaler.Execute}

	err := sut.background(MockBackgroundMessage{env, topic})

	require.NoError(t, err)

	require.Equal(t, 1, len(pub.Calls))
	require.Equal(t, topic, pub.Calls[0].Arguments[0])
	msg, ok := pub.Calls[0].Arguments[1].(*message.Message)
	require.True(t, ok)
	require.Equal(t, &marshaled, msg)
}

type MockBackgroundMessage struct {
	env   types.MessageEnvelope
	topic string
}

func (bm MockBackgroundMessage) Message() types.MessageEnvelope {
	return bm.env
}

func (bm MockBackgroundMessage) Topic() string {
	return bm.topic
}
