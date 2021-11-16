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
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
)

type RawInputWireFormat struct{}

func (*RawInputWireFormat) marshal(envelope types.MessageEnvelope, encryptor binaryModifier) (*message.Message, error) {
	return (&EdgeXWireFormat{}).marshal(envelope, encryptor)
}

func (*RawInputWireFormat) unmarshal(msg *message.Message, decryptor binaryModifier) (types.MessageEnvelope, error) {
	return (&RawWireFormat{}).unmarshal(msg, decryptor)
}

type RawOutputWireFormat struct{}

func (*RawOutputWireFormat) marshal(envelope types.MessageEnvelope, encryptor binaryModifier) (*message.Message, error) {
	return (&RawWireFormat{}).marshal(envelope, encryptor)
}

func (*RawOutputWireFormat) unmarshal(msg *message.Message, decryptor binaryModifier) (types.MessageEnvelope, error) {
	return (&EdgeXWireFormat{}).unmarshal(msg, decryptor)
}
