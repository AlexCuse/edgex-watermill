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
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/util"
	"github.com/edgexfoundry/go-mod-bootstrap/v2/bootstrap"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"strings"
	"sync"
)

type watermillTrigger struct {
	pub             message.Publisher
	sub             message.Subscriber
	marshaler       WatermillMarshaler
	unmarshaler     WatermillUnmarshaler
	encryptor       binaryModifier
	decryptor       binaryModifier
	topics          []string
	context         context.Context
	cancel          context.CancelFunc
	watermillConfig *WatermillConfigWrapper
	edgeXConfig     interfaces.TriggerConfig
}

func (t *watermillTrigger) input(watermillMessage *message.Message, receiveTopic string) {
	logger := t.edgeXConfig.Logger

	msg, err := t.unmarshaler(watermillMessage, t.decryptor)

	msg.ReceivedTopic = receiveTopic

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to unmarshal message: %s", err.Error()))
		watermillMessage.Nack()
		return
	}

	edgexContext := t.edgeXConfig.ContextBuilder(msg)

	logger.Trace("Received message", "topic", receiveTopic, common.CorrelationHeader, edgexContext.CorrelationID)

	//collect errors, consider failure if *any* pipeline fails on output
	err = t.edgeXConfig.MessageReceived(edgexContext, msg, t.output)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to process message: %s", err.Error()))
		watermillMessage.Nack()
		return
	}

	watermillMessage.Ack()
}

func (t *watermillTrigger) output(ctx interfaces.AppFunctionContext, pipeline *interfaces.FunctionPipeline) error {
	logger := ctx.LoggingClient()

	output := ctx.ResponseData()
	if output != nil && t.pub != nil {
		var err error
		pl := output

		if t.encryptor != nil {
			pl, err = t.encryptor(output)
		}

		if err != nil {
			return err
		}

		msg, err := t.marshaler(types.MessageEnvelope{
			CorrelationID: ctx.CorrelationID(),
			Payload:       pl,
			ContentType:   ctx.ResponseContentType(),
		}, t.encryptor)

		if err != nil {
			return err
		}

		publishTopic := t.watermillConfig.WatermillTrigger.PublishTopic

		err = t.pub.Publish(publishTopic, msg)

		if err != nil {
			return err
		}

		logger.Tracef("Published message to t output in pipeline %s (%s: %s, %s: %s)", pipeline.Id, "topic", publishTopic, common.CorrelationHeader, ctx.CorrelationID)
	}
	return nil
}

func (t *watermillTrigger) background(bg interfaces.BackgroundMessage) error {
	msg, err := t.marshaler(bg.Message(), t.encryptor)

	if err != nil {
		return err
	}

	err = t.pub.Publish(bg.Topic(), msg)

	if err != nil {
		return err
	}

	return nil
}

func (t *watermillTrigger) Initialize(wg *sync.WaitGroup, ctx context.Context, background <-chan interfaces.BackgroundMessage) (bootstrap.Deferred, error) {
	logger := t.edgeXConfig.Logger

	t.context, t.cancel = context.WithCancel(ctx)

	cfg := t.watermillConfig.WatermillTrigger

	logger.Info(fmt.Sprintf("Initializing t for '%s'", cfg.Type))

	logger.Info(fmt.Sprintf("Subscribing to topic: '%s' @ %s", cfg.SubscribeTopics, cfg.BrokerUrl))

	if len(strings.TrimSpace(cfg.SubscribeTopics)) == 0 {
		// Still allows subscribing to blank topic to receive all messages
		t.topics = append(t.topics, cfg.SubscribeTopics)
	} else {
		topics := util.DeleteEmptyAndTrim(strings.FieldsFunc(cfg.SubscribeTopics, util.SplitComma))
		for _, topic := range topics {
			t.topics = append(t.topics, topic)
		}
	}

	for _, topic := range t.topics {
		if si, ok := t.sub.(message.SubscribeInitializer); ok {
			err := si.SubscribeInitialize(topic)

			if err != nil {
				return nil, err
			}
		}

		tributary, err := t.sub.Subscribe(t.context, topic)

		if err != nil {
			return nil, err
		}

		wg.Add(1)

		go func(waitgroup *sync.WaitGroup, collectFrom <-chan *message.Message, topic string) {
			defer waitgroup.Done()
			for {
				select {
				case <-t.context.Done():
					return

				case m := <-collectFrom:
					go t.input(m, topic)

				}
			}
		}(wg, tributary, topic)
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-t.context.Done():
				return

			case bg := <-background:
				go t.background(bg)

			}
		}
	}()

	deferred := func() {
		logger.Info("Disconnecting t")
		if t.sub != nil {
			err := t.sub.Close()
			if err != nil {
				logger.Error("Unable to disconnect t Subscriber", "error", err.Error())
			}
		}

		if t.pub != nil {
			err := t.pub.Close()
			if err != nil {
				logger.Error("Unable to disconnect t Publisher", "error", err.Error())
			}
		}
	}

	return deferred, nil
}

func NewWatermillTrigger(publisher message.Publisher, subscriber message.Subscriber, format WireFormat, watermillConfig *WatermillConfigWrapper, edgeXConfig interfaces.TriggerConfig) (interfaces.Trigger, error) {
	t := &watermillTrigger{
		pub:             publisher,
		sub:             subscriber,
		watermillConfig: watermillConfig,
		edgeXConfig:     edgeXConfig,
		marshaler:       format.marshal,
		unmarshaler:     format.unmarshal,
		encryptor:       noopModifier,
		decryptor:       noopModifier,
	}

	var err error

	if watermillConfig != nil {
		protection, err := newAESProtection(&(watermillConfig.WatermillTrigger))

		if err == nil && protection != nil { // else err is going to be returned
			t.encryptor = protection.encrypt
			t.decryptor = protection.decrypt
		}
	}

	return t, err
}

func (t *watermillTrigger) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
}
