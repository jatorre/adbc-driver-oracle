// Copyright 2025 CARTO
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package oracle

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

func newTestCert(t *testing.T, cn string, isCA bool) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  isCA,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func pemBlock(typ string, der []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}))
}

func TestTLSConfigFromPEM(t *testing.T) {
	client, clientKey := newTestCert(t, "client", false)
	ca, _ := newTestCert(t, "Autonomous Database CA", true)
	pkcs8, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}

	// ewallet.pem order: key first, then the client certificate, then CAs.
	keyPKCS8 := pemBlock("PRIVATE KEY", pkcs8)
	keyPKCS1 := pemBlock("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(clientKey))
	certs := pemBlock("CERTIFICATE", ca.Raw) + pemBlock("CERTIFICATE", client.Raw)

	for name, content := range map[string]string{"pkcs8": keyPKCS8 + certs, "pkcs1": keyPKCS1 + certs} {
		t.Run(name, func(t *testing.T) {
			if !isPEMWallet(content) {
				t.Fatal("expected PEM wallet content to be detected")
			}
			cfg, err := tlsConfigFromPEM(content, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(cfg.Certificates) != 1 || !cfg.Certificates[0].Leaf.Equal(client) {
				t.Fatalf("expected the certificate matching the key as the client certificate")
			}
			if !cfg.InsecureSkipVerify {
				t.Fatal("expected verification to follow the verify argument")
			}
			if _, err := ca.Verify(x509.VerifyOptions{Roots: cfg.RootCAs}); err != nil {
				t.Fatalf("expected the bundled CA to be trusted: %v", err)
			}
		})
	}

	t.Run("verify", func(t *testing.T) {
		cfg, err := tlsConfigFromPEM(keyPKCS8+certs, true)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.InsecureSkipVerify {
			t.Fatal("expected server verification")
		}
	})
}

func TestTLSConfigFromPEMErrors(t *testing.T) {
	client, clientKey := newTestCert(t, "client", false)
	other, _ := newTestCert(t, "other", false)
	pkcs8, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	key := pemBlock("PRIVATE KEY", pkcs8)

	cases := map[string]struct{ content, want string }{
		"encrypted key": {pemBlock("ENCRYPTED PRIVATE KEY", []byte{0x30}) + pemBlock("CERTIFICATE", client.Raw), "encrypted"},
		"no key":        {pemBlock("CERTIFICATE", client.Raw), "no private key"},
		"no match":      {key + pemBlock("CERTIFICATE", other.Raw), "no certificate matches"},
		"two keys":      {key + key + pemBlock("CERTIFICATE", client.Raw), "more than one private key"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := tlsConfigFromPEM(tc.content, true)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestIsPEMWallet(t *testing.T) {
	if isPEMWallet("b2Jmc2NhdGVkIGN3YWxsZXQuc3Nv") {
		t.Fatal("base64 cwallet.sso must not be treated as PEM")
	}
	if !isPEMWallet("\n  -----BEGIN PRIVATE KEY-----\n") {
		t.Fatal("leading whitespace must not hide PEM content")
	}
}

func TestBuildDSNPEMWalletEnablesSSL(t *testing.T) {
	db := &databaseImpl{
		hostname: "adb.example.com", port: "1522", serviceName: "svc",
		user: "u", password: "p", walletContent: "-----BEGIN PRIVATE KEY-----",
	}
	dsn := db.buildDSN()
	if !strings.Contains(dsn, "SSL=enable") || !strings.Contains(dsn, "SSL+VERIFY=false") {
		t.Fatalf("expected SSL options in %s", dsn)
	}
}
