package store

import (
	"fmt"
)


type KVItem struct {
	Key       ByteString // these are KV pairs for underlying KV-store.
	Value     ByteString
	ValueExpr string // this is an expression that can use variables from the TX read-set
}

func (kv KVItem) String() string {
	return fmt.Sprintf("KVItem: {key: %v, Value: %v", kv.Key, kv.Value)
}