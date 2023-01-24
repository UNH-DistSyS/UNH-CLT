package config

type HealthMonitorConfig struct {
	HealthCheckHeartbeatIntervalMs int `json:"health_check_interval_ms"` // interval between healthcheck messages
	HealthCheckHeartbeatTimeoutlMs int `json:"health_check_timeout"`
	ResourceUsageCheckMultiple     int `json:"resource_usage_check_multiple"` // do a resource usage report every ResourceUsageCheckMultiple number of heartbeats
}

func NewDefaultHealthMonitorConfig() HealthMonitorConfig {
	return HealthMonitorConfig{
		HealthCheckHeartbeatIntervalMs: 10,
		HealthCheckHeartbeatTimeoutlMs: 50,
		ResourceUsageCheckMultiple:     100,
	}
}
