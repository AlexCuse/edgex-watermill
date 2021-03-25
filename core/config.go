package core

type WatermillConfigWrapper struct {
	WatermillTrigger WatermillConfig
}

type WatermillConfig struct {
	Type            string
	BrokerUrl       string
	ClientId        string
	SubscribeTopics string
	PublishTopic    string
	WatermillFormat string
	ConsumerGroup   string
	Optional        map[string]string
}

func (w *WatermillConfigWrapper) UpdateFromRaw(rawConfig interface{}) bool {
	rec, ok := rawConfig.(*WatermillConfigWrapper)

	if !ok {
		return false
	}

	*w = *rec

	return true
}
