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
	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/fxamacker/cbor/v2"
)

type EdgeXWireFormat struct {
}

func (*EdgeXWireFormat) marshal(envelope types.MessageEnvelope, encrypt binaryModifier) (*message.Message, error) {
	var pl []byte
	var err error

	//TODO: worth covering other content types?
	//is eg. CBOR in JSON envelope something we want to support?
	if envelope.ContentType == common.ContentTypeJSON {
		pl, err = json.Marshal(envelope)
	} else if envelope.ContentType == common.ContentTypeCBOR {
		pl, err = cbor.Marshal(envelope)
	}

	if err != nil {
		return nil, err
	}

	if encrypt != nil {
		pl, err = encrypt(pl)

		if err != nil {
			return nil, err
		}
	}

	msg := message.NewMessage(envelope.CorrelationID, pl)

	msg.Metadata.Set(middleware.CorrelationIDMetadataKey, envelope.CorrelationID)

	return msg, nil
}

func (*EdgeXWireFormat) unmarshal(message *message.Message, decrypt binaryModifier) (types.MessageEnvelope, error) {

	var err error

	env := types.MessageEnvelope{}

	pl := message.Payload

	if decrypt != nil {
		pl, err = decrypt(pl)
	}

	if err != nil {
		return env, err
	}

	//TODO: worth covering other content types?
	//is eg. CBOR in JSON envelope something we want to support?
	if pl[0] == byte('{') || pl[0] == byte('[') {
		err = json.Unmarshal(pl, &env)
	} else {
		err = cbor.Unmarshal(pl, &env)
	}

	return env, err
}
