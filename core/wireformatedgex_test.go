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
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEdgeXWireFormat_JSON_Marshal(t *testing.T) {
	env := types.MessageEnvelope{
		CorrelationID: uuid.New().String(),
		Payload:       []byte(`{ "S": "OK" }`),
		ContentType:   common.ContentTypeJSON,
	}

	jsn, _ := json.Marshal(env)

	sut := EdgeXWireFormat{}

	msg, err := sut.marshal(env, nil)

	require.Nil(t, err, "should not return error")

	require.NotNil(t, msg, "should return watermill message")

	require.Equal(t, string(jsn), string(msg.Payload), "should properly pass payload")
	require.Equal(t, env.CorrelationID, msg.UUID, "need an ID")
	require.Equal(t, env.CorrelationID, msg.Metadata.Get(middleware.CorrelationIDMetadataKey), "keep correlation ID for middleware routing if needed")
	require.Zero(t, msg.Metadata.Get(EdgeXContentType), "dont use metadata for edgex format")
}

func TestEdgeXWireFormat_JSON_Marshal_Encrypt(t *testing.T) {
	enc := []byte(uuid.NewString())

	env := types.MessageEnvelope{
		CorrelationID: uuid.New().String(),
		Payload:       []byte(`{ "S": "OK" }`),
		ContentType:   common.ContentTypeJSON,
	}

	jsn, _ := json.Marshal(env)

	sut := EdgeXWireFormat{}

	msg, err := sut.marshal(env, func(b []byte) ([]byte, error) {
		if bytes.Equal(b, jsn) {
			return enc, nil
		}
		return nil, fmt.Errorf("unexpected input to decrypt mock")
	})

	require.Nil(t, err, "should not return error")

	require.NotNil(t, msg, "should return watermill message")

	require.Equal(t, string(enc), string(msg.Payload), "should properly pass payload")
	require.Equal(t, env.CorrelationID, msg.UUID, "need an ID")
	require.Equal(t, env.CorrelationID, msg.Metadata.Get(middleware.CorrelationIDMetadataKey), "keep correlation ID for middleware routing if needed")
	require.Zero(t, msg.Metadata.Get(EdgeXContentType), "dont use metadata for edgex format")
}

func TestEdgeXWireFormat_JSON_Unmarshal(t *testing.T) {

	correlationID := uuid.New().String()

	env := types.MessageEnvelope{CorrelationID: correlationID, Payload: []byte("OK")}

	jsn, _ := json.Marshal(env)

	msg := message.NewMessage(uuid.New().String(), jsn)

	sut := EdgeXWireFormat{}

	result, err := sut.unmarshal(msg, nil)

	require.Nil(t, err, "should not return error")
	require.NotNil(t, result, "should return result")

	require.NotSame(t, &env, &result, "should not be the same object")
	require.Equal(t, env, result, "should properly pass payload")
	require.Equal(t, correlationID, result.CorrelationID, "should read correlation ID from metadata if present")
}

func TestEdgeXWireFormat_JSON_Unmarshal_Decrypt(t *testing.T) {
	enc := []byte(uuid.NewString())

	correlationID := uuid.New().String()

	env := types.MessageEnvelope{CorrelationID: correlationID, Payload: []byte("OK")}

	jsn, _ := json.Marshal(env)

	msg := message.NewMessage(uuid.New().String(), enc)

	sut := EdgeXWireFormat{}

	result, err := sut.unmarshal(msg, func(b []byte) ([]byte, error) {
		if bytes.Equal(b, enc) {
			return jsn, nil
		}
		return nil, fmt.Errorf("unexpected input to mock decrypt")
	})

	require.Nil(t, err, "should not return error")
	require.NotNil(t, result, "should return result")

	require.NotSame(t, &env, &result, "should not be the same object")
	require.Equal(t, env, result, "should properly pass payload")
	require.Equal(t, correlationID, result.CorrelationID, "should read correlation ID from metadata if present")
}

func TestEdgeXWireFormat_JSON_Unmarshal_JSONError(t *testing.T) {
	msg := message.NewMessage(uuid.New().String(), []byte("not json string"))

	sut := EdgeXWireFormat{}

	result, err := sut.unmarshal(msg, nil)

	require.NotNil(t, err, "should return error")
	require.Zero(t, result, "should not return result")
}
