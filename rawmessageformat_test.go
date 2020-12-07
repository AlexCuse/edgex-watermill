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
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/edgexfoundry/go-mod-core-contracts/clients"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRawMessageFormat_Marshal_HasCorrelationID(t *testing.T) {
	env := types.MessageEnvelope{
		Checksum:      uuid.New().String(),
		CorrelationID: uuid.New().String(),
		Payload:       []byte("OK"),
		ContentType:   uuid.New().String(),
	}

	sut := RawMessageFormat{}

	msg, err := sut.Marshal(env)

	require.Nil(t, err, "should not return error")

	require.NotNil(t, msg, "should return watermill message")

	require.Equal(t, string(env.Payload), string(msg.Payload), "should properly pass payload")
	require.Equal(t, env.CorrelationID, msg.UUID, "should use correlation ID as watermill message ID")
	require.Equal(t, env.CorrelationID, msg.Metadata.Get(middleware.CorrelationIDMetadataKey), "should use correlation ID as correlation ID for watermill router middleware")
	require.Equal(t, env.ContentType, msg.Metadata.Get(EdgeXContentType), "should store content type in metadata")
	require.Equal(t, env.Checksum, msg.Metadata.Get(EdgeXChecksum), "should store checksum in metadata")
}

func TestRawMessageFormat_Marshal_NoCorrelationId(t *testing.T) {
	env := types.MessageEnvelope{
		Checksum:      uuid.New().String(),
		CorrelationID: "",
		Payload:       []byte("OK"),
		ContentType:   uuid.New().String(),
	}

	sut := RawMessageFormat{}

	msg, err := sut.Marshal(env)

	require.Nil(t, err, "should not return error")

	require.NotNil(t, msg, "should return watermill message")

	require.Equal(t, string(env.Payload), string(msg.Payload), "should properly pass payload")
	require.NotZero(t, msg.UUID, "should set an arbitrary correlation ID / UUID if none provided")
	require.Equal(t, msg.UUID, msg.Metadata.Get(middleware.CorrelationIDMetadataKey), "UUID and correlation ID should match")
	require.Equal(t, env.ContentType, msg.Metadata.Get(EdgeXContentType), "should store content type in metadata")
	require.Equal(t, env.Checksum, msg.Metadata.Get(EdgeXChecksum), "should store checksum in metadata")
}

func TestRawMessageFormat_Unmarshal_HasCorrelationId(t *testing.T) {
	correlationID := uuid.New().String()
	contentType := uuid.New().String()
	checksum := uuid.New().String()

	msg := message.NewMessage(uuid.New().String(), []byte("OK"))
	msg.Metadata.Set(middleware.CorrelationIDMetadataKey, correlationID)
	msg.Metadata.Set(EdgeXContentType, contentType)
	msg.Metadata.Set(EdgeXChecksum, checksum)

	sut := RawMessageFormat{}

	env, err := sut.Unmarshal(msg)

	require.Nil(t, err, "should not return error")

	require.Equal(t, string(msg.Payload), string(env.Payload), "should properly pass payload")
	require.Equal(t, correlationID, env.CorrelationID, "should read correlation ID from metadata if present")
	require.Equal(t, checksum, env.Checksum, "should include checksum if passed in metadata")
	require.Equal(t, contentType, env.ContentType, "should include content type if passed in metadata")
}

func TestRawMessageFormat_Unmarshal_HasMessageID(t *testing.T) {
	messageID := uuid.New().String()
	contentType := uuid.New().String()
	checksum := uuid.New().String()

	msg := message.NewMessage(messageID, []byte("OK"))
	msg.Metadata.Set(EdgeXContentType, contentType)
	msg.Metadata.Set(EdgeXChecksum, checksum)

	sut := RawMessageFormat{}

	env, err := sut.Unmarshal(msg)

	require.Nil(t, err, "should not return error")

	require.Equal(t, string(msg.Payload), string(env.Payload), "should properly pass payload")
	require.Equal(t, messageID, env.CorrelationID, "should use watermill message ID as correlation ID if not present in metadata")
	require.Equal(t, checksum, env.Checksum, "should include checksum if passed in metadata")
	require.Equal(t, contentType, env.ContentType, "should include content type if passed in metadata")
}

func TestRawMessageFormat_Marshal_HasNoID(t *testing.T) {
	contentType := uuid.New().String()
	checksum := uuid.New().String()

	msg := message.NewMessage("", []byte("OK"))
	msg.Metadata.Set(EdgeXContentType, contentType)
	msg.Metadata.Set(EdgeXChecksum, checksum)

	sut := RawMessageFormat{}

	env, err := sut.Unmarshal(msg)

	require.Nil(t, err, "should not return error")

	require.Equal(t, string(msg.Payload), string(env.Payload), "should properly pass payload")
	require.NotZero(t, env.CorrelationID, "should use assign correlation ID if not present in metadata or watermill message ID")
	require.Equal(t, checksum, env.Checksum, "should include checksum if passed in metadata")
	require.Equal(t, contentType, env.ContentType, "should include content type if passed in metadata")
}

func TestRawMessageFormat_Unmarshal_InfersCBORByDefault(t *testing.T) {
	correlationID := uuid.New().String()
	checksum := uuid.New().String()

	msg := message.NewMessage(uuid.New().String(), []byte("OK"))
	msg.Metadata.Set(middleware.CorrelationIDMetadataKey, correlationID)
	msg.Metadata.Set(EdgeXChecksum, checksum)

	sut := RawMessageFormat{}

	env, err := sut.Unmarshal(msg)

	require.Nil(t, err, "should not return error")

	require.Equal(t, string(msg.Payload), string(env.Payload), "should properly pass payload")
	require.Equal(t, correlationID, env.CorrelationID, "should read correlation ID from metadata if present")
	require.Equal(t, checksum, env.Checksum, "should include checksum if passed in metadata")
	require.Equal(t, clients.ContentTypeCBOR, env.ContentType, "should include content type if passed in metadata")
}

func TestRawMessageFormat_Unmarshal_InfersJSONForObject(t *testing.T) {
	correlationID := uuid.New().String()
	checksum := uuid.New().String()

	msg := message.NewMessage(uuid.New().String(), []byte("{OK"))
	msg.Metadata.Set(middleware.CorrelationIDMetadataKey, correlationID)
	msg.Metadata.Set(EdgeXChecksum, checksum)

	sut := RawMessageFormat{}

	env, err := sut.Unmarshal(msg)

	require.Nil(t, err, "should not return error")

	require.Equal(t, string(msg.Payload), string(env.Payload), "should properly pass payload")
	require.Equal(t, correlationID, env.CorrelationID, "should read correlation ID from metadata if present")
	require.Equal(t, checksum, env.Checksum, "should include checksum if passed in metadata")
	require.Equal(t, clients.ContentTypeJSON, env.ContentType, "should include content type if passed in metadata")
}

func TestRawMessageFormat_Unmarshal_InfersJSONForArray(t *testing.T) {
	correlationID := uuid.New().String()
	checksum := uuid.New().String()

	msg := message.NewMessage(uuid.New().String(), []byte("[OK"))
	msg.Metadata.Set(middleware.CorrelationIDMetadataKey, correlationID)
	msg.Metadata.Set(EdgeXChecksum, checksum)

	sut := RawMessageFormat{}

	env, err := sut.Unmarshal(msg)

	require.Nil(t, err, "should not return error")

	require.Equal(t, string(msg.Payload), string(env.Payload), "should properly pass payload")
	require.Equal(t, correlationID, env.CorrelationID, "should read correlation ID from metadata if present")
	require.Equal(t, checksum, env.Checksum, "should include checksum if passed in metadata")
	require.Equal(t, clients.ContentTypeJSON, env.ContentType, "should include content type if passed in metadata")
}
