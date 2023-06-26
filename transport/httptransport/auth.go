package httptransport

import "encoding/base64"

func BasicAuthHeader(username, password string) string {
	return "Basic " + BasicAuth(username, password)
}

// Copied from
// https://github.com/golang/go/blob/16d6a5233a183be7264295c66167d35c689f9372/src/net/http/client.go#L413-L421
//
// See 2 (end of page 4) https://www.ietf.org/rfc/rfc2617.txt
// "To receive authorization, the client sends the userid and password,
// separated by a single colon (":") character, within a base64
// encoded string in the credentials."
// It is not meant to be urlencoded.
func BasicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}
