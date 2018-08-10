package message

import "time"

// Message represents a single message sent/consumed from local kafka
type Message struct {
	Id        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}
