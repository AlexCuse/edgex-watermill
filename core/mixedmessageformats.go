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

type RawInputMessageFormat struct{}

func (*RawInputMessageFormat) marshal(envelope types.MessageEnvelope) (*message.Message, error) {
	return (&EdgeXMessageFormat{}).marshal(envelope)
}

func (*RawInputMessageFormat) unmarshal(msg *message.Message) (types.MessageEnvelope, error) {
	return (&RawMessageFormat{}).unmarshal(msg)
}

type RawOutputMessageFormat struct{}

func (*RawOutputMessageFormat) marshal(envelope types.MessageEnvelope) (*message.Message, error) {
	return (&RawMessageFormat{}).marshal(envelope)
}

func (*RawOutputMessageFormat) unmarshal(msg *message.Message) (types.MessageEnvelope, error) {
	return (&EdgeXMessageFormat{}).unmarshal(msg)
}
