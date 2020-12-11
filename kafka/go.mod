module github.com/alexcuse/edgex-watermill/kafka

go 1.15

replace github.com/edgexfoundry/app-functions-sdk-go => github.com/alexcuse/app-functions-sdk-go v1.3.1-dev.5.0.20201211224301-c9c9efe5a898

require (
	github.com/Shopify/sarama v1.27.2
	github.com/ThreeDotsLabs/watermill v1.1.1
	github.com/ThreeDotsLabs/watermill-kafka/v2 v2.2.0
	github.com/alexcuse/edgex-watermill v0.0.0-20201211230458-144f56209b15
	github.com/edgexfoundry/app-functions-sdk-go v1.3.0
	github.com/edgexfoundry/go-mod-messaging v0.1.28
	github.com/google/uuid v1.1.2
	github.com/stretchr/testify v1.6.1
)
