// Copyright 2025 CARTO
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oracle

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
)

// isPEMWallet reports whether wallet_content carries PEM text rather than a
// base64-encoded cwallet.sso.
func isPEMWallet(content string) bool {
	return strings.HasPrefix(strings.TrimSpace(content), "-----BEGIN")
}

// tlsConfigFromPEM builds the mutual-TLS client configuration from the
// contents of an Autonomous Database ewallet.pem whose private key is not
// encrypted. The certificate whose public key matches the private key is the
// client certificate; every other certificate in the bundle is trusted as a
// server root on top of the system pool.
func tlsConfigFromPEM(content string, verify bool) (*tls.Config, error) {
	var certs []*x509.Certificate
	var key crypto.Signer

	rest := []byte(content)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		switch block.Type {
		case "CERTIFICATE":
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("wallet PEM: invalid certificate: %w", err)
			}
			certs = append(certs, cert)
		case "ENCRYPTED PRIVATE KEY":
			return nil, errors.New("wallet PEM: the private key is encrypted; decrypt it before passing it as " + OptionWalletContent)
		case "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY":
			if key != nil {
				return nil, errors.New("wallet PEM: more than one private key")
			}
			parsed, err := parsePrivateKey(block)
			if err != nil {
				return nil, err
			}
			key = parsed
		}
	}

	if key == nil {
		return nil, errors.New("wallet PEM: no private key")
	}

	var leaf *x509.Certificate
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	for _, cert := range certs {
		if leaf == nil && publicKeysEqual(cert.PublicKey, key.Public()) {
			leaf = cert
			continue
		}
		roots.AddCert(cert)
	}
	if leaf == nil {
		return nil, errors.New("wallet PEM: no certificate matches the private key")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{leaf.Raw},
			PrivateKey:  key,
			Leaf:        leaf,
		}},
		RootCAs:            roots,
		InsecureSkipVerify: !verify,
	}, nil
}

func parsePrivateKey(block *pem.Block) (crypto.Signer, error) {
	var parsed any
	var err error
	switch block.Type {
	case "RSA PRIVATE KEY":
		parsed, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		parsed, err = x509.ParseECPrivateKey(block.Bytes)
	default:
		parsed, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	}
	if err != nil {
		return nil, fmt.Errorf("wallet PEM: invalid private key: %w", err)
	}
	signer, ok := parsed.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("wallet PEM: unsupported private key type %T", parsed)
	}
	return signer, nil
}

func publicKeysEqual(a, b crypto.PublicKey) bool {
	eq, ok := a.(interface{ Equal(crypto.PublicKey) bool })
	return ok && eq.Equal(b)
}
