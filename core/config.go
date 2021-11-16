package core

type WatermillConfigWrapper struct {
	WatermillTrigger WatermillConfig
}

type WatermillConfig struct {
	Type                string
	BrokerUrl           string
	ClientId            string
	SubscribeTopics     string
	PublishTopic        string
	WireFormat          string
	ConsumerGroup       string
	Optional            map[string]string
	EncryptionAlgorithm string
	EncryptionKey       string
}

func (w *WatermillConfigWrapper) UpdateFromRaw(rawConfig interface{}) bool {
	rec, ok := rawConfig.(*WatermillConfigWrapper)

	if !ok {
		return false
	}

	w.WatermillTrigger = rec.WatermillTrigger

	return true
}
