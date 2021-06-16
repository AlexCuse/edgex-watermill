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
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/google/uuid"
)

type RawWireFormat struct{}

func (*RawWireFormat) marshal(envelope types.MessageEnvelope) (*message.Message, error) {
	correlationID := envelope.CorrelationID

	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	m := message.NewMessage(correlationID, envelope.Payload)

	m.Metadata.Set(EdgeXContentType, envelope.ContentType)
	m.Metadata.Set(middleware.CorrelationIDMetadataKey, correlationID)

	return m, nil
}

func (*RawWireFormat) unmarshal(msg *message.Message) (types.MessageEnvelope, error) {
	correlationID := msg.Metadata.Get(middleware.CorrelationIDMetadataKey)

	if correlationID == "" {
		correlationID = msg.UUID
	}

	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	contentType := msg.Metadata.Get(EdgeXContentType)

	if contentType == "" {
		if msg.Payload[0] == byte('{') || msg.Payload[0] == byte('[') {
			contentType = common.ContentTypeJSON
		} else {
			contentType = common.ContentTypeCBOR
		}
	}

	formattedMessage := types.MessageEnvelope{
		Payload:       msg.Payload,
		CorrelationID: correlationID,
		ContentType:   contentType,
	}
	return formattedMessage, nil
}
