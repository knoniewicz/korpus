package channel

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
)

var sonicJSON = sonic.ConfigFastest

var (
	entityTableNameRegex = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	eventPool            = sync.Pool{
		New: func() interface{} {
			return &Event{
				Payload: make(map[string]interface{}, 16),
			}
		},
	}
)

type Event struct {
	Key        string
	Action     string
	RequestID  string
	Payload    map[string]interface{}
	retryCount int
}
type Envelope struct {
	RequestID string                 `json:"request_id"`
	Payload   map[string]interface{} `json:"payload"`
}

func (e *Event) String() string {
	return fmt.Sprintf("Key: %s, Action: %s, RequestID: %s, Payload: %v", e.Key, e.Action, e.RequestID, e.Payload)
}

func NewEvent(msg *redis.Message) (*Event, error) {
	e := eventPool.Get().(*Event)
	if err := e.Parse(msg.Channel); err != nil {
		eventPool.Put(e)
		return nil, fmt.Errorf("parse error: %w", err)
	}

	var env Envelope
	if err := sonicJSON.UnmarshalFromString(strings.TrimSpace(msg.Payload), &env); err != nil {
		eventPool.Put(e) // Put back on error
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	e.RequestID = env.RequestID
	e.Payload = env.Payload

	return e, nil
}

func (e *Event) Release() {
	e.Key = ""
	e.Action = ""
	e.RequestID = ""
	e.retryCount = 0
	for k := range e.Payload {
		delete(e.Payload, k)
	}
	eventPool.Put(e)
}

func (e *Event) Parse(channel string) error {
	i := strings.LastIndexByte(channel, '.')
	if i == -1 {
		return fmt.Errorf("invalid channel format: %s (expected key.action)", channel)
	}
	e.Key = channel[:i]
	e.Action = channel[i+1:]
	return nil
}

func ValidateTableName(tableName string) error {
	if !entityTableNameRegex.MatchString(tableName) {
		return fmt.Errorf("invalid entity/table name %q: must match ^[a-z_][a-z0-9_]*$", tableName)
	}
	return nil
}

func GetSonicJSON() sonic.API {
	return sonicJSON
}

func (e *Event) IncrementRetry() int {
	e.retryCount++
	return e.retryCount
}

func (e *Event) RetryCount() int {
	return e.retryCount
}
