package p2p

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
)

func IssueREST(secret []byte, sessionID, connectionID string, epoch uint64, nowUnix, ttlSeconds int64) (string, string) {
	user := fmt.Sprintf("%d:%s_%s_%d", nowUnix+ttlSeconds, sessionID, connectionID, epoch)
	mac := hmac.New(sha1.New, secret)
	_, _ = mac.Write([]byte(user))
	return user, base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
