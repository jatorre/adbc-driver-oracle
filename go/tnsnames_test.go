// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package oracle

import "testing"

func TestParseTNSNames(t *testing.T) {
	const tnsnames = `
# Wallet shipped by Autonomous Database
acmefreetier_high = (description= (retry_count=20)(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=rlg3b4hox1h62gb_high.adb.oraclecloud.com)))
acmefreetier_medium = (description= (retry_count=20)(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=rlg3b4hox1h62gb_medium.adb.oraclecloud.com)))
ACMEFREETIER_LOW = (description= (retry_count=20)(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=rlg3b4hox1h62gb_low.adb.oraclecloud.com)))
`

	got := parseTNSNames(tnsnames)
	if len(got) != 3 {
		t.Fatalf("expected 3 aliases, got %d (%v)", len(got), got)
	}

	cases := map[string]struct{ host, port, service string }{
		"acmefreetier_high":   {"adb.us-ashburn-1.oraclecloud.com", "1522", "rlg3b4hox1h62gb_high.adb.oraclecloud.com"},
		"acmefreetier_medium": {"adb.us-ashburn-1.oraclecloud.com", "1522", "rlg3b4hox1h62gb_medium.adb.oraclecloud.com"},
		"acmefreetier_low":    {"adb.us-ashburn-1.oraclecloud.com", "1522", "rlg3b4hox1h62gb_low.adb.oraclecloud.com"},
	}
	for alias, want := range cases {
		entry, ok := got[alias]
		if !ok {
			t.Errorf("alias %q not found", alias)
			continue
		}
		if entry.Host != want.host || entry.Port != want.port || entry.Service != want.service {
			t.Errorf("alias %q: got %+v want %+v", alias, entry, want)
		}
	}
}

func TestParseTNSNamesIgnoresIncomplete(t *testing.T) {
	got := parseTNSNames(`bare = (description=())`)
	if len(got) != 0 {
		t.Errorf("expected to skip alias with no host/port/service, got %v", got)
	}
}
