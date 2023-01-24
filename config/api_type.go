package config

type ApiType int

const (
	KV 			ApiType = iota
	CASSANDRA
)