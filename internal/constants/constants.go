package constants

import "errors"

// Database field names
const (
	FieldRequestID = "request_id"
	FieldEndedAt   = "ended_at"
	FieldCreatedAt = "created_at"
	FieldUpdatedAt = "updated_at"
	FieldDeletedAt = "deleted_at"
	FieldID        = "id"
)

// Event actions
const (
	ActionCreated = "created"
	ActionEnded   = "ended"
	ActionUpdated = "updated"
	ActionDeleted = "deleted"
)

// Channel prefixes and patterns
const (
	ChannelPrefixKorpus = "korpus."
)

// HTTP headers
const (
	HeaderContentType   = "Content-Type"
	HeaderKorpusToken   = "x-korpus-token"
	HeaderXRequestID    = "X-Request-ID"
	HeaderAccessControl = "Access-Control-Allow-Origin"
)

// Content types
const (
	ContentTypeJSON = "application/json"
)

// Status strings
const (
	StatusSuccess     = "success"
	StatusError       = "error"
	StatusSchemaError = "schema_error"
	StatusUnknown     = "unknown"
)

// JSON schema keys
const (
	SchemaKeyProperties   = "properties"
	SchemaKeyRequired     = "required"
	SchemaKeyType         = "type"
	SchemaKeyFormat       = "format"
	SchemaKeyPrimaryKey   = "x-primary-key"
	SchemaKeyUnique       = "x-unique"
	SchemaKeyForeignKey   = "x-foreign-key"
	SchemaKeyXLookup      = "x-lookup"
	SchemaKeyXChildEntity = "x-child-entity"
	SchemaKeyXVector      = "x-vector"
	SchemaKeyXSession     = "x-session"
)

// JSON schema types
const (
	JSONTypeString     = "string"
	JSONTypeArray      = "array"
	JSONTypeObject     = "object"
	JSONTypeNumber     = "number"
	JSONTypeBoolean    = "boolean"
	JSONTypeNull       = "null"
	JSONFormatDateTime = "date-time"
)

// HTTP error messages
const (
	ErrUnauthorized           = "unauthorized"
	ErrRateLimitExceeded      = "rate limit exceeded"
	ErrInternalServerError    = "internal server error"
	ErrInvalidPath            = "invalid path"
	ErrFailedToGetEntities    = "failed to get entities"
	ErrFailedToGetTableSchema = "failed to get table schema"
	ErrFailedToGetSchema      = "failed to get schema"
	ErrInvalidBody            = "invalid body"
)

// Context keys
const (
	CtxKeyRequestID = "request_id"
)

// Health check status
const (
	HealthStatusOK        = "ok"
	HealthStatusDegraded  = "degraded"
	HealthStatusHealthy   = "healthy"
	HealthStatusUnhealthy = "unhealthy"
	HealthStatusDatabase  = "database"
	HealthStatusRedis     = "redis"
	HealthStatusStatus    = "status"
)

// Database operations
const (
	DBOpInsertEvent = "insert_event"
	DBOpSoftDelete  = "soft_delete"
	DBOpGetEntities = "get_entities"
)

// Channel queue names
const (
	QueueJobs         = "jobs"
	QueueRedis        = "redis"
	QueueWrite        = "write"
	QueueServerEvents = "server_events"
)

// Query parameter names
const (
	ParamLimit           = "limit"
	ParamOffset          = "offset"
	ParamFields          = "fields"
	ParamSort            = "sort"
	ParamResolve         = "resolve"
	ParamDepth           = "depth"
	ParamIncludeChildren = "include_children"
	ParamService         = "service"
	ParamEntity          = "entity"
)

// Errors
var (
	ErrRetryableInsert = errors.New("retryable insert")
)
