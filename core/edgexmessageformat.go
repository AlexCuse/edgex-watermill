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
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/clients"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/fxamacker/cbor/v2"
)

type EdgeXMessageFormat struct{}

func (*EdgeXMessageFormat) marshal(envelope types.MessageEnvelope) (*message.Message, error) {
	var pl []byte
	var err error

	//TODO: worth covering other content types?
	//is eg. CBOR in JSON envelope something we want to support?
	if envelope.ContentType == clients.ContentTypeJSON {
		pl, err = json.Marshal(envelope)
	} else if envelope.ContentType == clients.ContentTypeCBOR {
		pl, err = cbor.Marshal(envelope)
	}

	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(envelope.CorrelationID, pl)

	msg.Metadata.Set(middleware.CorrelationIDMetadataKey, envelope.CorrelationID)

	return msg, nil
}

func (*EdgeXMessageFormat) unmarshal(message *message.Message) (types.MessageEnvelope, error) {

	var err error

	env := types.MessageEnvelope{}

	//TODO: worth covering other content types?
	//is eg. CBOR in JSON envelope something we want to support?
	if message.Payload[0] == byte('{') || message.Payload[0] == byte('[') {
		err = json.Unmarshal(message.Payload, &env)
	} else {
		err = cbor.Unmarshal(message.Payload, &env)
	}

	return env, err
}
