module github.com/alexcuse/edgex-watermill

go 1.15

replace github.com/edgexfoundry/app-functions-sdk-go => github.com/alexcuse/app-functions-sdk-go v1.3.1-dev.5.0.20201212001952-f81c328e6690

require (
	github.com/ThreeDotsLabs/watermill v1.1.1
	github.com/edgexfoundry/app-functions-sdk-go v1.3.0
	github.com/edgexfoundry/go-mod-bootstrap v0.0.61
	github.com/edgexfoundry/go-mod-core-contracts v0.1.121
	github.com/edgexfoundry/go-mod-messaging v0.1.28
	github.com/google/uuid v1.1.2
	github.com/hashicorp/go-multierror v1.0.0
	github.com/stretchr/testify v1.6.1
)
