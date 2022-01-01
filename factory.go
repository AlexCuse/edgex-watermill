package edgex_watermill

import (
	"fmt"
	"github.com/alexcuse/edgex-watermill/v2/amqp"
	"github.com/alexcuse/edgex-watermill/v2/core"
	"github.com/alexcuse/edgex-watermill/v2/googlecloud"
	"github.com/alexcuse/edgex-watermill/v2/jetstream"
	"github.com/alexcuse/edgex-watermill/v2/kafka"
	"github.com/alexcuse/edgex-watermill/v2/nats"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"strings"
)

func Register(service interfaces.ApplicationService) {
	service.RegisterCustomTriggerFactory("watermill", buildTrigger)
}

func buildTrigger(config interfaces.TriggerConfig) (interfaces.Trigger, error) {
	cfg := &core.WatermillConfigWrapper{}

	err := config.ConfigLoader(cfg, "WatermillTrigger")

	if err != nil {
		return nil, err
	}

	switch strings.ToLower(cfg.WatermillTrigger.Type) {
	case "nats":
		return nats.Trigger(cfg, config)
	case "jetstream":
		return jetstream.Trigger(cfg, config)
	case "kafka":
		return kafka.Trigger(cfg, config)
	case "amqp":
		return amqp.Trigger(cfg, config)
	case "googlecloud":
		return googlecloud.Trigger(cfg, config)
	default:
		return nil, fmt.Errorf("Invalid Trigger Type Specified: %s", cfg.WatermillTrigger.Type)
	}
}
