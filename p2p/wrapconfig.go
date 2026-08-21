package p2p

import (
	"bytes"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats-server/v2/conf"
)

// SplitP2PBlock extracts a top-level p2p { ... } block from a combined config file.
// The remaining bytes are suitable for server.ProcessConfigFile.
func SplitP2PBlock(src []byte) (natsConf []byte, p2p Config, hasP2P bool, err error) {
	blockStart, blockEnd, found, err := findTopLevelP2PBlock(src)
	if err != nil {
		return nil, Config{}, false, err
	}
	if !found {
		return append([]byte(nil), src...), Config{}, false, nil
	}

	openBrace := bytes.IndexByte(src[blockStart:blockEnd], '{')
	if openBrace < 0 {
		return nil, Config{}, false, fmt.Errorf("p2p block missing '{'")
	}
	inner := src[blockStart+openBrace+1 : blockEnd-1]
	m, err := conf.Parse(string(inner))
	if err != nil {
		return nil, Config{}, true, fmt.Errorf("parse p2p block: %w", err)
	}
	cfg, err := configFromMap(m)
	if err != nil {
		return nil, Config{}, true, err
	}

	out := make([]byte, 0, len(src)-(blockEnd-blockStart))
	out = append(out, src[:blockStart]...)
	out = append(out, src[blockEnd:]...)
	return out, cfg, true, nil
}

func findTopLevelP2PBlock(src []byte) (blockStart, blockEnd int, found bool, err error) {
	depth := 0
	inString := byte(0)
	lineComment := false

	for i := 0; i < len(src); i++ {
		if lineComment {
			if src[i] == '\n' {
				lineComment = false
			}
			continue
		}
		if inString != 0 {
			if src[i] == '\\' && i+1 < len(src) {
				i++
				continue
			}
			if src[i] == inString {
				inString = 0
			}
			continue
		}

		switch src[i] {
		case '"', '\'':
			inString = src[i]
		case '#':
			lineComment = true
		case '/':
			if i+1 < len(src) && src[i+1] == '/' {
				lineComment = true
				i++
			}
		case '{':
			depth++
		case '}':
			depth--
			if depth < 0 {
				return 0, 0, false, fmt.Errorf("unexpected '}' at offset %d", i)
			}
		default:
			if depth == 0 && matchesKeyword(src, i, "p2p") {
				j := skipSpace(src, i+3)
				if j >= len(src) || src[j] != '{' {
					continue
				}
				end, err := matchingBrace(src, j)
				if err != nil {
					return 0, 0, false, err
				}
				return i, end + 1, true, nil
			}
		}
	}
	if depth != 0 {
		return 0, 0, false, fmt.Errorf("unclosed block")
	}
	return 0, 0, false, nil
}

func matchesKeyword(src []byte, i int, word string) bool {
	if i > 0 {
		prev := src[i-1]
		if isIdentByte(prev) {
			return false
		}
	}
	if i+len(word) > len(src) {
		return false
	}
	if !bytes.EqualFold(src[i:i+len(word)], []byte(word)) {
		return false
	}
	if i+len(word) < len(src) && isIdentByte(src[i+len(word)]) {
		return false
	}
	return true
}

func isIdentByte(b byte) bool {
	return b == '_' || b == '-' || unicode.IsLetter(rune(b)) || unicode.IsDigit(rune(b))
}

func skipSpace(src []byte, i int) int {
	for i < len(src) {
		switch src[i] {
		case ' ', '\t', '\r', '\n':
			i++
		default:
			return i
		}
	}
	return i
}

func matchingBrace(src []byte, open int) (int, error) {
	if open >= len(src) || src[open] != '{' {
		return 0, fmt.Errorf("expected '{' at offset %d", open)
	}
	depth := 0
	inString := byte(0)
	lineComment := false

	for i := open; i < len(src); i++ {
		if lineComment {
			if src[i] == '\n' {
				lineComment = false
			}
			continue
		}
		if inString != 0 {
			if src[i] == '\\' && i+1 < len(src) {
				i++
				continue
			}
			if src[i] == inString {
				inString = 0
			}
			continue
		}

		switch src[i] {
		case '"', '\'':
			inString = src[i]
		case '#':
			lineComment = true
		case '/':
			if i+1 < len(src) && src[i+1] == '/' {
				lineComment = true
				i++
			}
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return 0, fmt.Errorf("unclosed p2p block")
}

func configFromMap(m map[string]any) (Config, error) {
	cfg := Config{CredentialTTL: DefaultCredentialTTL}
	var err error

	if v, ok := m["stun_urls"]; ok {
		cfg.STUNURLs = stringSlice(v)
	}
	if v, ok := m["turn_urls"]; ok {
		cfg.TURNURLs = stringSlice(v)
	}
	if v, ok := m["secret_file"]; ok {
		cfg.SecretFile, err = stringValue(v)
		if err != nil {
			return Config{}, fmt.Errorf("secret_file: %w", err)
		}
	}
	if v, ok := m["credential_ttl"]; ok {
		cfg.CredentialTTL, err = durationValue(v)
		if err != nil {
			return Config{}, fmt.Errorf("credential_ttl: %w", err)
		}
	}
	if v, ok := m["sys_username"]; ok {
		cfg.SysUsername, err = stringValue(v)
		if err != nil {
			return Config{}, fmt.Errorf("sys_username: %w", err)
		}
	}
	if v, ok := m["sys_password"]; ok {
		cfg.SysPassword, err = stringValue(v)
		if err != nil {
			return Config{}, fmt.Errorf("sys_password: %w", err)
		}
	}
	if v, ok := m["agent_account"]; ok {
		cfg.AgentAccount, err = stringValue(v)
		if err != nil {
			return Config{}, fmt.Errorf("agent_account: %w", err)
		}
	}
	if v, ok := m["agent_username"]; ok {
		cfg.AgentUsername, err = stringValue(v)
		if err != nil {
			return Config{}, fmt.Errorf("agent_username: %w", err)
		}
	}
	if v, ok := m["agent_password"]; ok {
		cfg.AgentPassword, err = stringValue(v)
		if err != nil {
			return Config{}, fmt.Errorf("agent_password: %w", err)
		}
	}
	return cfg, nil
}

func stringSlice(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		return []string{t}
	default:
		return nil
	}
}

func stringValue(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	default:
		return "", fmt.Errorf("expected string, got %T", v)
	}
}

func durationValue(v any) (time.Duration, error) {
	switch t := v.(type) {
	case string:
		d, err := time.ParseDuration(strings.TrimSpace(t))
		if err != nil {
			return 0, err
		}
		return d, nil
	case int64:
		return time.Duration(t), nil
	case float64:
		return time.Duration(t), nil
	default:
		return 0, fmt.Errorf("expected duration, got %T", v)
	}
}
