//
// Copyright (c) 2021 Alex Ullrich
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
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/v3/pkg/interfaces"
	"github.com/edgexfoundry/app-functions-sdk-go/v3/pkg/util"
)

type WatermillSender interface {
	Send(interfaces.AppFunctionContext, interface{}) (bool, interface{})
}

type watermillSender struct {
	pub              message.Publisher
	encryptor        binaryModifier
	baseTopic        string
	continuePipeline bool
}

func NewWatermillSender(pub message.Publisher, proceed bool, config *WatermillConfig) (WatermillSender, error) {
	if config == nil {
		return nil, fmt.Errorf("missing watermill config")
	}

	s := &watermillSender{
		pub:              pub,
		encryptor:        noopModifier,
		baseTopic:        config.PublishTopic,
		continuePipeline: proceed,
	}

	var protection dataProtection
	protection, err := newAESProtection(config)

	if err == nil && protection != nil { // else err is going to be returned
		s.encryptor = protection.encrypt
	}

	return s, err
}

func (ws *watermillSender) Send(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	bytes, err := util.CoerceType(data)

	if err != nil {
		return false, fmt.Errorf("Invalid message received for %s (%T) - could not convert to byte array for publishing", ctx.CorrelationID(), data)
	}

	ebytes, err := ws.encryptor(bytes)

	if err != nil {
		return false, fmt.Errorf("Failed to encrypt data.")
	}

	topic, err := ctx.ApplyValues(ws.baseTopic)

	if err != nil {
		return false, err
	}

	err = ws.pub.Publish(topic, message.NewMessage(ctx.CorrelationID(), ebytes))

	if err != nil {
		return false, nil
	}

	return ws.continuePipeline, nil
}
