package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/alexcuse/edgex-watermill/v2/core"
	"github.com/alexcuse/edgex-watermill/v2/jetstream"
	"github.com/edgexfoundry/app-functions-sdk-go/v3/pkg"
	"github.com/edgexfoundry/app-functions-sdk-go/v3/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/common"
	"github.com/edgexfoundry/go-mod-messaging/v3/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/google/uuid"
)

const (
	serviceKey = "edgex-watermill-producer"
)

type sender struct {
	client messaging.MessageClient
	topic  string
}

type message struct {
	Name string `json:"name"`
	Body string `json:"body"`
}

func main() {
	// 1) First thing to do is to create an instance of the EdgeX SDK and initialize it.
	service, ok := pkg.NewAppServiceWithTargetType(serviceKey, &message{})
	if !ok {
		fmt.Println("SDK initialization failed")
		os.Exit(-1)
	}

	appSettings := service.ApplicationSettings()

	client, err := jetstream.Client(context.Background(),
		core.WatermillConfig{
			Type:                appSettings["MessageBusType"],
			BrokerUrl:           appSettings["BrokerUrl"],
			ClientId:            appSettings["ClientID"],
			PublishTopic:        appSettings["WireFormat"],
			WireFormat:          appSettings["WireFormat"],
			EncryptionAlgorithm: appSettings["EncryptionAlgorithm"],
			EncryptionKey:       appSettings["EncryptionKey"],
		})

	if err != nil {
		panic(err)
	}

	s := sender{
		topic:  appSettings["MessageBusPublishTopic"],
		client: client,
	}

	service.SetFunctionsPipeline(
		buildMessageEnvelope,
		s.send,
	)

	service.MakeItRun()

	os.Exit(0)
}

func (s sender) send(edgexcontext interfaces.AppFunctionContext, param interface{}) (bool, interface{}) {
	env := param.(types.MessageEnvelope)

	err := s.client.Publish(env, s.topic)

	if err != nil {
		edgexcontext.LoggingClient().Errorf("Failed to publish: %s", err.Error())
	}

	return false, nil
}

func buildMessageEnvelope(edgexcontext interfaces.AppFunctionContext, param interface{}) (bool, interface{}) {
	jsn, err := json.Marshal(param)

	if err != nil {
		edgexcontext.LoggingClient().Errorf("Unable to marshal payload: %s (%+v)", err, param)
		return false, err
	}

	correlationId := edgexcontext.CorrelationID()
	if len(correlationId) == 0 {
		correlationId = uuid.New().String()
	}

	env := types.MessageEnvelope{
		CorrelationID: correlationId,
		Payload:       jsn,
		ContentType:   common.ContentTypeJSON,
	}

	return true, env
}
