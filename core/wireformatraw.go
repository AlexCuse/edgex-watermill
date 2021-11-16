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

func (*RawWireFormat) marshal(envelope types.MessageEnvelope, encrypt binaryModifier) (*message.Message, error) {
	correlationID := envelope.CorrelationID

	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	pl := envelope.Payload

	if encrypt != nil {
		var err error
		pl, err = encrypt(pl)

		if err != nil {
			return nil, err
		}
	}

	m := message.NewMessage(correlationID, pl)

	m.Metadata.Set(EdgeXContentType, envelope.ContentType)
	m.Metadata.Set(middleware.CorrelationIDMetadataKey, correlationID)

	return m, nil
}

func (*RawWireFormat) unmarshal(msg *message.Message, decrypt binaryModifier) (types.MessageEnvelope, error) {
	correlationID := msg.Metadata.Get(middleware.CorrelationIDMetadataKey)

	if correlationID == "" {
		correlationID = msg.UUID
	}

	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	contentType := msg.Metadata.Get(EdgeXContentType)

	pl := msg.Payload

	var err error

	if decrypt != nil {
		pl, err = decrypt(pl)
	}

	if err != nil {
		return types.MessageEnvelope{}, err
	}

	if contentType == "" {
		if pl[0] == byte('{') || pl[0] == byte('[') {
			contentType = common.ContentTypeJSON
		} else {
			contentType = common.ContentTypeCBOR
		}
	}

	formattedMessage := types.MessageEnvelope{
		Payload:       pl,
		CorrelationID: correlationID,
		ContentType:   contentType,
	}
	return formattedMessage, nil
}
