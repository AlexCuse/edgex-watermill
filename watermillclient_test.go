//
// Copyright (c) 2020 Alex Ullrich
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

package edgex_watermill

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewWatermillClient(t *testing.T) {
	format := &RawMessageFormat{}

	client, err := NewWatermillClient(context.Background(), nil, nil, format)

	require.Nil(t, err, "should not return error")
	require.NotNil(t, client, "should return initialized client")

	require.NotNil(t, client.(*watermillClient).marshaler, "should assign marshaler from format")
}

func TestNewWatermillClient_Default_EdgexJSON(t *testing.T) {
	client, err := NewWatermillClient(context.Background(), nil, nil, nil)

	require.Nil(t, err, "should not return error")
	require.NotNil(t, client, "should return initialized client")

	require.NotNil(t, client.(*watermillClient).marshaler, "should assign marshaler from format")
}
