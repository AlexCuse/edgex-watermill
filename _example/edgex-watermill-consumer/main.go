package main

import (
	"fmt"
	"os"

	ewm "github.com/alexcuse/edgex-watermill/v2"
	"github.com/edgexfoundry/app-functions-sdk-go/v3/pkg"
	"github.com/edgexfoundry/app-functions-sdk-go/v3/pkg/interfaces"
)

const (
	serviceKey = "edgex-watermill-consumer"
)

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

	ewm.Register(service)

	service.SetFunctionsPipeline(
		printMessage,
	)

	service.MakeItRun()

	os.Exit(0)
}

var cnt = 0

func printMessage(edgexcontext interfaces.AppFunctionContext, param interface{}) (bool, interface{}) {
	cnt++
	edgexcontext.LoggingClient().Infof("message received: %+v (%d)", param, cnt)
	return false, nil
}
