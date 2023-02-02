package config

type ClientConfig struct {
	// Client Specific Configs
	CqlParseAtClient       bool `json:"cql_parse_at_client"`    // whether we parse cql at client or node_assembler
	ClientRequestTimeoutMS int  `json:"client_request_timeout"` // timeout for clients in milliseconds
}

func NewDefaultClientConfig() *ClientConfig {
	config := new(ClientConfig)
	config.CqlParseAtClient = true
	config.ClientRequestTimeoutMS = 1000

	return config
}
