package core

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/clients/logger"
)

type edgexWatermillAdapter struct {
	client logger.LoggingClient
	fields watermill.LogFields
}

var x watermill.LoggerAdapter

func NewLogAdapter(client logger.LoggingClient, fields watermill.LogFields) watermill.LoggerAdapter {
	return &edgexWatermillAdapter{
		client: client,
		fields: fields,
	}
}

func (ewa *edgexWatermillAdapter) Error(msg string, err error, fields watermill.LogFields) {
	ewa.client.Errorf("%s (%s) - %+v", msg, err.Error(), ewa.combineFields(fields))
}

func (ewa *edgexWatermillAdapter) Info(msg string, fields watermill.LogFields) {
	ewa.client.Infof("%s - %+v", msg, ewa.combineFields(fields))
}

func (ewa *edgexWatermillAdapter) Debug(msg string, fields watermill.LogFields) {
	ewa.client.Debugf("%s - %+v", msg, ewa.combineFields(fields))
}

func (ewa *edgexWatermillAdapter) Trace(msg string, fields watermill.LogFields) {
	ewa.client.Tracef("%s - %+v", msg, ewa.combineFields(fields))
}

func (ewa *edgexWatermillAdapter) combineFields(passedFields watermill.LogFields) watermill.LogFields {
	out := make(map[string]interface{})

	for k, v := range passedFields {
		out[k] = v
	}

	for k, v := range ewa.fields {
		if _, found := out[k]; !found {
			out[k] = v
		}
	}

	return out
}

func (ewa *edgexWatermillAdapter) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return NewLogAdapter(ewa.client, ewa.combineFields(fields))
}
