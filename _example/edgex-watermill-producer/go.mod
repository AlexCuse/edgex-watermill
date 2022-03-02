module edgex-watermill-consumer

go 1.16

replace github.com/alexcuse/edgex-watermill/v2 => ../../

require (
	github.com/AlexCuse/watermill-jetstream v0.0.3-unstable.1 // indirect
	github.com/alexcuse/edgex-watermill/v2 v2.0.0-20211121113746-806b2111adf9
	github.com/edgexfoundry/app-functions-sdk-go/v2 v2.2.0-dev.28
	github.com/edgexfoundry/go-mod-bootstrap/v2 v2.0.1-dev.28
	github.com/edgexfoundry/go-mod-core-contracts/v2 v2.2.0-dev.11
	github.com/edgexfoundry/go-mod-messaging/v2 v2.2.0-dev.8
	github.com/google/uuid v1.3.0
	github.com/nats-io/nats.go v1.13.1-0.20220202232944-a0a6a71ede98
)
