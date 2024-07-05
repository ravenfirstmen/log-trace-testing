package messaging

import (
	"fmt"
	"github.com/nats-io/nats.go"
)

type NatsHeaderCarrier nats.Header

// Get returns the value associated with the passed key.
func (hc NatsHeaderCarrier) Get(key string) string {
	return nats.Header(hc).Get(key)
}

// Set stores the key-value pair.
func (hc NatsHeaderCarrier) Set(key string, value string) {
	nats.Header(hc).Set(key, value)
}

// Keys lists the keys stored in this carrier.
func (hc NatsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(hc))
	for k := range hc {
		keys = append(keys, k)
	}
	return keys
}

// // MY DEBUGGER
type DebuggerCarrier struct{}

// Keys lists the keys stored in this carrier.
func (hc DebuggerCarrier) Keys() []string {
	return []string{}
}

func (hc DebuggerCarrier) Get(key string) string {
	return ""
}

// Set stores the key-value pair.
func (hc DebuggerCarrier) Set(key string, value string) {
	fmt.Printf("CARRIER: %s = %s\n", key, value)
}
