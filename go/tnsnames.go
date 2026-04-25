// Package oracle implements an ADBC driver for Oracle.
//
// This file: minimal tnsnames.ora parser used to resolve a TNS alias to its
// host/port/service when the user passes the alias as oracle.dsn alongside an
// oracle.wallet_location. Oracle Autonomous Database connection wallets ship
// with tnsnames.ora that maps friendly aliases like "acmefreetier_medium" to
// the underlying TCPS endpoint, so honoring those aliases lets users paste in
// the same connection string they'd use with sqlplus or sqlcl.

package oracle

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type tnsEntry struct {
	Host    string
	Port    string
	Service string
}

// loadTNSAliases reads <walletDir>/tnsnames.ora and returns a map from
// lower-cased alias name → host/port/service. Returns nil with no error when
// walletDir is empty or the file does not exist.
func loadTNSAliases(walletDir string) (map[string]tnsEntry, error) {
	if walletDir == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(walletDir, "tnsnames.ora"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseTNSNames(string(data)), nil
}

// parseTNSNames is a tiny, regex-based parser sufficient for the
// alias = (description=...(host=...)(port=...)(service_name=...)) shape that
// Oracle wallet tnsnames.ora files use. It is not a full implementation of
// the Oracle Net naming format; it deliberately only extracts the three
// fields the driver needs to construct an oracle:// URL.
func parseTNSNames(text string) map[string]tnsEntry {
	cleaned := stripTNSComments(text)
	out := map[string]tnsEntry{}

	// Match top-level alias entries: an identifier followed by `= (`. Aliases
	// can be dotted (DOMAIN style) — allow those characters too.
	aliasRe := regexp.MustCompile(`(?im)^[ \t]*([A-Za-z0-9_.]+)[ \t]*=[ \t]*\(`)
	for _, m := range aliasRe.FindAllStringSubmatchIndex(cleaned, -1) {
		alias := cleaned[m[2]:m[3]]
		// The opening paren is the byte right before m[1].
		openParen := m[1] - 1
		closeParen := matchParen(cleaned, openParen)
		if closeParen == -1 {
			continue
		}
		block := cleaned[openParen : closeParen+1]
		entry := tnsEntry{
			Host:    extractTNSParam(block, "host"),
			Port:    extractTNSParam(block, "port"),
			Service: extractTNSParam(block, "service_name"),
		}
		if entry.Host != "" && entry.Port != "" && entry.Service != "" {
			out[strings.ToLower(alias)] = entry
		}
	}
	return out
}

// stripTNSComments removes everything after `#` on each line and replaces it
// with whitespace so byte offsets in the resulting string still match those
// of the input.
func stripTNSComments(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, line := range strings.SplitAfter(text, "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			b.WriteString(line[:i])
			b.WriteString(strings.Repeat(" ", len(line)-i))
		} else {
			b.WriteString(line)
		}
	}
	return b.String()
}

// matchParen returns the byte offset of the `)` that balances the `(` at
// `open`, or -1 if the input is unbalanced.
func matchParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// extractTNSParam pulls the value of `(key=value)` out of a tnsnames block.
// The match is case-insensitive and value collection stops at the next `)`,
// so values that themselves contain parentheses are not supported (they do
// not appear in the host/port/service positions).
func extractTNSParam(block, key string) string {
	re := regexp.MustCompile(`(?i)\(\s*` + regexp.QuoteMeta(key) + `\s*=\s*([^)]+)\)`)
	m := re.FindStringSubmatch(block)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimSpace(m[1])
}
