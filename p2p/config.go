package p2p

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const DefaultCredentialTTL = 24 * time.Hour

type Config struct {
	STUNURLs       []string
	TURNURLs       []string
	SecretFile     string
	CredentialTTL  time.Duration
}

func ValidateConfig(cfg Config) error {
	for _, raw := range cfg.STUNURLs {
		if err := validateICEURL(raw, false); err != nil {
			return fmt.Errorf("stun url %q: %w", raw, err)
		}
	}
	for _, raw := range cfg.TURNURLs {
		if err := validateICEURL(raw, true); err != nil {
			return fmt.Errorf("turn url %q: %w", raw, err)
		}
	}
	return nil
}

func validateICEURL(raw string, isTURN bool) error {
	scheme, host, query, err := parseICEURL(raw)
	if err != nil {
		return err
	}
	if isTURN {
		if scheme != "turn" {
			return errors.New("expected turn scheme")
		}
		if !hasTransportUDP(query) {
			return errors.New("transport=udp required")
		}
	} else if scheme != "stun" {
		return errors.New("expected stun scheme")
	}
	if isLoopbackHost(host) {
		return errors.New("loopback host not allowed")
	}
	return nil
}

func parseICEURL(raw string) (scheme, host, query string, err error) {
	colon := strings.Index(raw, ":")
	if colon <= 0 {
		return "", "", "", errors.New("invalid url")
	}
	scheme = raw[:colon]
	if scheme != "stun" && scheme != "turn" {
		return "", "", "", errors.New("invalid scheme")
	}

	rest := raw[colon+1:]
	if q := strings.Index(rest, "?"); q >= 0 {
		query = rest[q+1:]
		rest = rest[:q]
	}

	hostPort := rest
	lastColon := strings.LastIndex(hostPort, ":")
	if lastColon < 0 {
		host = hostPort
	} else {
		host = hostPort[:lastColon]
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return "", "", "", errors.New("missing host")
	}
	return scheme, host, query, nil
}

func hasTransportUDP(query string) bool {
	for _, part := range strings.Split(query, "&") {
		if part == "transport=udp" {
			return true
		}
	}
	return false
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func LoadSecret(path string) ([]byte, error) {
	return os.ReadFile(path)
}
