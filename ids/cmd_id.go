package ids

// CommandID identifies commands from each id, can be any integer type.
type CommandID uint64

func NewCommandId(nodeId ID, cmdIdCounter int) CommandID {
	var cmdId uint64 = 0
	cmdId = uint64(nodeId.ZoneId)
	cmdId = cmdId << 8
	cmdId = cmdId | uint64(nodeId.NodeId)
	cmdId = cmdId << 24
	cmdId = cmdId | uint64(cmdIdCounter)
	return CommandID(cmdId)
}
