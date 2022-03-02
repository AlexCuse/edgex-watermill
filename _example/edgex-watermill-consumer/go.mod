module edgex-watermill-consumer

go 1.16

replace github.com/alexcuse/edgex-watermill/v2 => ../../

require (
	github.com/AlexCuse/watermill-jetstream v0.0.3-unstable.1 // indirect
	github.com/alexcuse/edgex-watermill/v2 v2.0.0-20211121113746-806b2111adf9
	github.com/edgexfoundry/app-functions-sdk-go/v2 v2.2.0-dev.28
)
