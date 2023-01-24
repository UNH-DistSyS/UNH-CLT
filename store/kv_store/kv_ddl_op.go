package kv_store
import "github.com/google/uuid"

// in KV we cn only create tables, and table have no schema
type KvDdlOp struct {
	TableName string
	TableId   uuid.UUID
}

