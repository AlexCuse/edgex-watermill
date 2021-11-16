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
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/AlexCuse/etm"
	"strings"
)

type binaryModifier func([]byte) ([]byte, error)

func noopModifier(b []byte) ([]byte, error) {
	return b, nil
}

type dataProtection interface {
	encrypt([]byte) ([]byte, error)
	decrypt([]byte) ([]byte, error)
}

func newAESProtection(watermillConfig *WatermillConfig) (protection dataProtection, err error) {
	if alg := watermillConfig.EncryptionAlgorithm; alg != "" {
		key, err := hex.DecodeString(watermillConfig.EncryptionKey)

		if err != nil {
			return nil, err
		}

		protection = &aesProtection{
			alg: alg,
			key: key,
		}
	}
	return protection, err
}

type aesProtection struct {
	alg string
	key []byte
}

func (ap aesProtection) encrypt(bytes []byte) ([]byte, error) {
	dst := make([]byte, 0)

	aead, err := getAead(ap.alg, ap.key)

	if err != nil {
		return dst, err
	}

	nonce := make([]byte, aead.NonceSize())
	_, err = rand.Read(nonce)

	if err != nil {
		return dst, err
	}

	dst = aead.Seal(dst, nonce, bytes, nil)

	fmt.Println("encrypted length: ", len(dst))

	return dst, err
}

func (ap aesProtection) decrypt(bytes []byte) ([]byte, error) {
	aead, err := getAead(ap.alg, ap.key)

	if err != nil {
		return nil, err
	}

	return aead.Open(make([]byte, 0), nil, bytes, nil)
}

const (
	AES256SHA512 = "aes256-sha512"
	AES256SHA384 = "aes256-sha384"
	AES192SHA384 = "aes192-sha384"
	AES128SHA256 = "es128-sha256"
)

func getAead(alg string, key []byte) (cipher.AEAD, error) {
	switch strings.ToLower(alg) {
	case AES128SHA256:
		return etm.NewAES128SHA256(key)
	case AES192SHA384:
		return etm.NewAES192SHA384(key)
	case AES256SHA384:
		return etm.NewAES256SHA384(key)
	case AES256SHA512:
		return etm.NewAES256SHA512(key)
	default:
		return nil, fmt.Errorf("invalid algorithm specified: %s", alg)
	}
}
