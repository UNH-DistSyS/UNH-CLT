package config

type OverloadPreventingBalancerConfig struct {
	CpuThreshold     uint8 `json:"cpu_threshold"`     // the cutoff for CPU utilization after which we do not migrate to node
	MemThreshold     uint8 `json:"mem_threshold"`     // same as above for RAM
	StorageThreshold uint8 `json:"storage_threshold"` // same as above for Disk
}

func NewDefaultOverloadPreventingBalancerConfig() OverloadPreventingBalancerConfig {
	return OverloadPreventingBalancerConfig{
		CpuThreshold:     90,
		MemThreshold:     90,
		StorageThreshold: 90,
	}
}
