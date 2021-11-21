//
// Copyright (c) 2021 Alex Ullrich
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package core

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/clients/logger"
)

type edgexWatermillAdapter struct {
	client logger.LoggingClient
	fields watermill.LogFields
}

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
