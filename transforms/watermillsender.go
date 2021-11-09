package transforms

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/util"
)

type watermillSender struct {
	pub message.Publisher
	baseTopic        string
	continuePipeline bool
}

func NewWatermillSender() *watermillSender  {
	return &watermillSender{}
}

func (ws *watermillSender) Send(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	bytes, err := util.CoerceType(data)

	if err != nil {
		return false, fmt.Errorf("Invalid message received for %s (%T) - could not convert to byte array for publishing", ctx.CorrelationID(), data)
	}

	topic, err := ctx.ApplyValues(ws.baseTopic)

	if err != nil {
		return false, err
	}

	err = ws.pub.Publish(topic, message.NewMessage(ctx.CorrelationID(), bytes))

	if err != nil {
		return false, nil
	}

	return ws.continuePipeline, nil
}