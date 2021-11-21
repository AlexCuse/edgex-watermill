package core

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/util"
)

type WatermillSender interface {
	Send(interfaces.AppFunctionContext, interface{})(bool, interface{})
}

type watermillSender struct {
	pub              message.Publisher
	encryptor		 binaryModifier
	baseTopic        string
	continuePipeline bool
}

func NewWatermillSender(pub message.Publisher, proceed bool, config *WatermillConfig) (WatermillSender, error) {
	var err error

	s := &watermillSender{
		pub: pub,
		encryptor: noopModifier,
		baseTopic: config.PublishTopic,
		continuePipeline: proceed,
	}

	if config != nil {
		protection, err := newAESProtection(config)

		if err == nil && protection != nil { // else err is going to be returned
			s.encryptor = protection.encrypt
		}
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
