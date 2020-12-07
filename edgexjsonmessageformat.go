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
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
)

type EdgeXJSONMessageFormat struct{}

func (*EdgeXJSONMessageFormat) Marshal(envelope types.MessageEnvelope) (*message.Message, error) {
	jsn, err := json.Marshal(envelope)

	if err != nil {
		return nil, err
	}

	return message.NewMessage(envelope.CorrelationID, jsn), nil
}

func (*EdgeXJSONMessageFormat) Unmarshal(message *message.Message) (types.MessageEnvelope, error) {
	env := types.MessageEnvelope{}

	err := json.Unmarshal(message.Payload, &env)

	return env, err
}
