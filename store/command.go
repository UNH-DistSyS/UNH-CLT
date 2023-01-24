package store

import (
	"encoding/gob"
	"fmt"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/google/uuid"
)

func init() {
	gob.Register(KVTableSpec{})
}

type OperationType uint8

const (
	NONE        OperationType = iota // no operation
	NOOP                             // an operation that does nothing
	WRITE                            // used for writes, some updates, and some deletes. This would be translated down to KV layer
	UPDATE                           // While KV layer updates can be expressed as WRITE, some CQL updates requires retrieving data first
	DELETE                           // While KV layer deletes can be expressed as WRITE nil, higher layers' deletes are more complex
	READ                             // used to replicate reads - essentially orders reads in the log. Reads are at the KV layer with translation to higher APIs as needed
	DDL_WRITE                        // used for commands that impact schema
	MINI_TX                          // used for more complex operations combining reads and write
	RECOVER_NIL                      // used for recovering status
)

type Command struct {
	CommandID         ids.CommandID
	ConflictDomainKey ByteString // uniquely identifies a conflict domain, such as a table partition
	Version           uint64     // for versioned store
	OperationType     OperationType
	Operation         interface{}
	TargetStore       StoreType
}

func (c Command) String() string {
	switch c.OperationType {
	case NONE:
		return fmt.Sprint("NONE")
	case READ:
		return fmt.Sprintf("READ{CD=%x, id=%v}", c.ConflictDomainKey, c.CommandID)
	case WRITE:
		return fmt.Sprintf("WRITE{CD=%x, id=%v, Op=%v}", c.ConflictDomainKey, c.CommandID, c.Operation)
	case UPDATE:
		return fmt.Sprintf("UPDATE{CD=%x, id=%v}", c.ConflictDomainKey, c.CommandID)
	case DELETE:
		return fmt.Sprintf("DELETE{CD=%x, id=%v}", c.ConflictDomainKey, c.CommandID)
	case NOOP:
		return fmt.Sprintf("NOOP{CD=%x, id=%v}", c.ConflictDomainKey, c.CommandID)
	case MINI_TX:
		return fmt.Sprintf("MINI_TX{CD=%x, id=%v}", c.ConflictDomainKey, c.CommandID)
	case DDL_WRITE:
		return fmt.Sprintf("DDL_WRITE{CD=%x id=%v}", c.ConflictDomainKey, c.CommandID)
	default:
		return fmt.Sprintf("UNKNOWN_CMD{CD=%x, id=%v}", c.ConflictDomainKey, c.CommandID)
	}
}

func (c *Command) Equal(a Command) bool {
	return c.CommandID == a.CommandID && c.ConflictDomainKey.B64() == a.ConflictDomainKey.B64()
}

func (c *Command) CreateResult(result interface{}, tableId uuid.UUID) *CommandResult {
	return &CommandResult{
		CommandID:         c.CommandID,
		TableId:           tableId,
		ConflictDomainKey: c.ConflictDomainKey,
		OperationType:     c.OperationType,
		Result:            result,
		Version:           c.Version,
	}
}

type KVTableSpec struct {
	TableUUID uuid.UUID
	TableName string
}

type CommandResult struct {
	CommandID         ids.CommandID
	TableId           uuid.UUID
	ConflictDomainKey ByteString
	OperationType     OperationType
	Result            interface{}
	Version           uint64 // this represents the version of the result. It is equivalent to the replication log's slot #
}

func (c CommandResult) String() string {
	switch c.OperationType {
	case READ:
		return fmt.Sprintf("READ_RESULT{key=%v, Table=%s, id=%v}", string(c.ConflictDomainKey), c.TableId, c.CommandID)
	case WRITE:
		return fmt.Sprintf("WRITE_RESULT{key=%v, Table=%s, id=%v}", string(c.ConflictDomainKey), c.TableId, c.CommandID)
	default:
		return fmt.Sprintf("UNKNOWN_CMD_RESULT{key=%v, Table=%s, id=%v}", string(c.ConflictDomainKey), c.TableId, c.CommandID)
	}
}

func (c CommandResult) Equal(a CommandResult) bool {
	return c.TableId == a.TableId && c.ConflictDomainKey.B64() == a.ConflictDomainKey.B64() && c.CommandID == a.CommandID
}
