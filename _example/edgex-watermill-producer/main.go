package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alexcuse/edgex-watermill/v2/core"
	"github.com/alexcuse/edgex-watermill/v2/jetstream"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/google/uuid"
	"os"
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

	factory := NewJetstreamStoreFactory(serviceKey)

	service.RegisterCustomStoreFactory("jetstream", factory.NewStore)

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

var sent = 0

func (s sender) send(edgexcontext interfaces.AppFunctionContext, param interface{}) (bool, interface{}) {
	sent = sent + 1
	if _, ok := param.(types.MessageEnvelope); ok {
		retryData, err := json.Marshal(param)

		if err != nil {
			edgexcontext.LoggingClient().Errorf("failed to unmarshal: %s", err.Error())
		}

		edgexcontext.SetRetryData(retryData)

		return false, fmt.Errorf("cant process now setting retry data (%d)", sent)
	}

	bytesFromStoreForward := param.([]byte)

	var env types.MessageEnvelope

	err := json.Unmarshal(bytesFromStoreForward, &env)

	if err != nil {
		return false, err
	}

	err = s.client.Publish(env, s.topic)

	if err != nil {
		edgexcontext.LoggingClient().Errorf("Failed to publish: %s (%d)", err.Error(), sent)
	} else {
		edgexcontext.LoggingClient().Infof("Published on retry (%d)", sent)
	}

	return false, err
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
