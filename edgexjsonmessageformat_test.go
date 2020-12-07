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
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEdgeXJSONMessageFormat_Marshal(t *testing.T) {
	env := types.MessageEnvelope{
		Checksum:      uuid.New().String(),
		CorrelationID: uuid.New().String(),
		Payload:       []byte("OK"),
		ContentType:   uuid.New().String(),
	}

	jsn, _ := json.Marshal(env)

	sut := EdgeXJSONMessageFormat{}

	msg, err := sut.Marshal(env)

	require.Nil(t, err, "should not return error")

	require.NotNil(t, msg, "should return watermill message")

	require.Equal(t, string(jsn), string(msg.Payload), "should properly pass payload")
	require.Equal(t, env.CorrelationID, msg.UUID, "need an ID")
	require.Zero(t, msg.Metadata.Get(middleware.CorrelationIDMetadataKey), "dont use metadata for raw format")
	require.Zero(t, msg.Metadata.Get(EdgeXContentType), "dont use metadata for raw format")
	require.Zero(t, msg.Metadata.Get(EdgeXChecksum), "dont use metadata for raw format")
}

func TestEdgeXJSONMessageFormat_Unmarshal(t *testing.T) {
	correlationID := uuid.New().String()

	env := types.MessageEnvelope{CorrelationID: correlationID, Payload: []byte("OK")}

	jsn, _ := json.Marshal(env)

	msg := message.NewMessage(uuid.New().String(), jsn)

	sut := EdgeXJSONMessageFormat{}

	result, err := sut.Unmarshal(msg)

	require.Nil(t, err, "should not return error")
	require.NotNil(t, result, "should return result")

	require.NotSame(t, &env, &result, "should not be the same object")
	require.Equal(t, env, result, "should properly pass payload")
	require.Equal(t, correlationID, result.CorrelationID, "should read correlation ID from metadata if present")
}

func TestEdgeXJSONMessageFormat_Unmarshal_JSONError(t *testing.T) {
	msg := message.NewMessage(uuid.New().String(), []byte("not json string"))

	sut := EdgeXJSONMessageFormat{}

	result, err := sut.Unmarshal(msg)

	require.NotNil(t, err, "should return error")
	require.Zero(t, result, "should not return result")
}
