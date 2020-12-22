module github.com/alexcuse/edgex-watermill/kafka

go 1.15

replace github.com/edgexfoundry/app-functions-sdk-go => github.com/alexcuse/app-functions-sdk-go v1.3.1-dev.5.0.20201222162257-083785a4b71c

require (
	github.com/Shopify/sarama v1.27.2
	github.com/ThreeDotsLabs/watermill v1.1.1
	github.com/ThreeDotsLabs/watermill-kafka/v2 v2.2.0
	github.com/alexcuse/edgex-watermill/core v0.0.0-20201222161157-2b007353ff83
	github.com/edgexfoundry/app-functions-sdk-go v1.3.0
	github.com/edgexfoundry/go-mod-messaging v0.1.28
	github.com/google/uuid v1.1.2
	github.com/stretchr/testify v1.6.1
)
