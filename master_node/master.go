package master_node

import (
	"bytes"
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/messages"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
	scp "github.com/bramvdbogaerde/go-scp"
	"github.com/bramvdbogaerde/go-scp/auth"
	"golang.org/x/crypto/ssh"
)

type Master struct {
	netman     netwrk.Communicator
	cfg        *config.Config
	id         ids.ID
	msgCounter int
	filetype   string
	replyChans map[int]chan bool
	sync.Mutex
}

func NewMaster(cfg *config.Config, identity *ids.ID) *Master {
	opDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewMasterCommunicator(cfg, *identity, opDispatcher)
	m := Master{
		netman:     netman,
		cfg:        cfg,
		id:         *identity,
		filetype:   ".csv",
		replyChans: make(map[int]chan bool),
	}
	log.Infof("Master is %v", m)
	m.netman.Register(messages.ReplyToMaster{}, m.HandleReply)

	return &m
}

func (m *Master) SetFileType(filetype string) {
	m.filetype = filetype
}

func (m *Master) HandleReply(ctx context.Context, msg messages.ReplyToMaster) {
	m.Lock()
	replyChan := m.replyChans[msg.ID]
	m.Unlock()
	log.Infof("Master %v received reply %v", m.id, msg)
	if replyChan == nil {
		log.Errorf("Reply chan is nil")
	}
	replyChan <- msg.Ok
}

func (m *Master) broadcastMsg(id int, msg interface{}) bool {
	log.Debugf("Master %s is sending msg %v", m.id, msg)
	m.Lock()
	replyCh := make(chan bool, m.cfg.ChanBufferSize)
	log.Debugf("Master making reply chan for msgId %d", id)
	m.replyChans[id] = replyCh
	m.Unlock()
	m.netman.Broadcast(msg, false) // broadcast msg
	expected := len(m.cfg.ClusterMembership.Addrs)
	received := 0
	for {
		select {
		case ok := <-replyCh:
			received++
			log.Debugf("Master received %d OKs", received)
			if !ok {
				m.Lock()
				m.replyChans[id] = nil
				m.Unlock()
				return ok
			}
			if expected <= received {
				m.Lock()
				m.replyChans[id] = nil
				m.Unlock()
				return true
			}
		case <-time.After(time.Millisecond * time.Duration(m.cfg.CommunicationTimeoutMs)):
			log.Debugf("message %v timeout waiting for reply (timout=%d ms)", id, m.cfg.CommunicationTimeoutMs)
			m.Lock()
			m.replyChans[id] = nil
			m.Unlock()
			return false
		}
	}
}

func (m *Master) Run() {
	m.netman.Run()
}

func (m *Master) Close() {
	m.netman.Close()
}

func (m *Master) BroadcastConfig() bool {
	m.Mutex.Lock()
	msg := messages.NewConfigMsg(m.cfg)
	m.msgCounter++
	m.Mutex.Unlock()
	return m.broadcastMsg(msg.ID, msg)

}
func (m *Master) Start(testDuration int) bool {
	if !m.BroadcastConfig() {
		log.Errorln("BroadcastConfig failed!")
		return false
	}

	m.Mutex.Lock()
	msg := messages.StartLatencyTest{
		ID:                    m.msgCounter,
		TestingDurationSecond: testDuration,
	}
	m.msgCounter++
	m.Mutex.Unlock()
	return m.broadcastMsg(msg.ID, msg)
}

func (m *Master) Stop() bool {

	m.Mutex.Lock()
	msg := messages.StopLatencyTest{
		ID:    m.msgCounter,
		Close: false,
	}
	m.msgCounter++
	m.Mutex.Unlock()
	return m.broadcastMsg(msg.ID, msg)
}

func (m *Master) CloseNodes() bool {
	m.Mutex.Lock()
	msg := messages.StopLatencyTest{
		ID:    m.msgCounter,
		Close: true,
	}
	m.msgCounter++
	m.Mutex.Unlock()
	return m.broadcastMsg(msg.ID, msg)
}
func (m *Master) getIdxFromResponse(response string, id string) (int, int) {
	file_names := strings.Fields(response)
	idxs := make([]int, 0)
	for _, file_name := range file_names {
		sub := strings.Replace(file_name, m.filetype, "", -1)
		subb := strings.Replace(sub, "testing_"+id+"_", "", -1)
		idx, err := strconv.Atoi(subb)
		if err != nil {
			return 0, 0
		}
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)

	if len(idxs) > 0 {
		return idxs[0], idxs[len(idxs)-1]
	}

	return 0, 0
}

func (m *Master) getAddress(id ids.ID) string {
	return strings.Split(strings.Replace(m.cfg.ClusterMembership.Addrs[id].PrivateAddress, "tcp://", "", -1), ":")[0]
}

func (m *Master) doSSH(id ids.ID, cmd string) string {
	// var hostKey ssh.PublicKey
	key, err := os.ReadFile(m.cfg.ClusterMembership.Addrs[id].RSA_path)
	if err != nil {
		log.Errorf("unable to read private key: %v", err)
		return ""
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Errorf("unable to parse private key: %v %v", err)
		return ""
	}
	config := &ssh.ClientConfig{
		User: m.cfg.ClusterMembership.Addrs[id].Usr_name,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	client, err := ssh.Dial("tcp", m.getAddress(id)+":22", config)
	if err != nil {
		log.Errorf("unable to connect: %v", err)
		return ""
	}
	defer client.Close()
	session, _ := client.NewSession()
	defer session.Close()

	var b bytes.Buffer
	session.Stdout = &b
	if err := session.Run(cmd); err != nil {
		log.Errorln("Failed to run: " + err.Error() + cmd)
	}
	return b.String()
}

func (m *Master) GetFileIdxes(id ids.ID) (int, int) {
	command := "ls " + m.cfg.ClusterMembership.Addrs[id].CSV_prefix
	start, end := m.getIdxFromResponse(m.doSSH(id, command), id.String())
	return start, end
}

func (m *Master) CopyFileFromRemoteToLocal(id ids.ID, start int, end int, path string) {
	// estabilsh connections
	for idx := start; idx < end; idx++ {
		clientConfig, _ := auth.PrivateKey(m.cfg.ClusterMembership.Addrs[id].Usr_name, m.cfg.ClusterMembership.Addrs[id].RSA_path, ssh.InsecureIgnoreHostKey())
		client := scp.NewClient(m.getAddress(id)+":22", &clientConfig)
		err := client.Connect()
		if err != nil {
			log.Errorln("Couldn't establish scp connection to the remote server ", err)
		}
		os.MkdirAll(path, 0755)
		filename := "testing_" + id.String() + "_" + strconv.Itoa(idx) + m.filetype
		file, err := os.Create(path + filename)
		if err != nil {
			log.Errorln(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.CommunicationTimeoutMs)*time.Millisecond*100)
		defer cancel()
		err = client.CopyFromRemote(ctx, file, m.cfg.ClusterMembership.Addrs[id].CSV_prefix+filename)
		if err != nil {
			log.Errorln("error copying file: ", err)
		}
		client.Close()
		file.Close()
	}
}

func (m *Master) DeleteFile(id ids.ID, start int, end int) {
	for idx := start; idx < end; idx++ {
		command := "rm " + m.cfg.ClusterMembership.Addrs[id].CSV_prefix + "testing_" + id.String() + "_" + strconv.Itoa(idx) + m.filetype
		m.doSSH(id, command)
	}
}

func (m *Master) Download(duration int) {
	ticker := time.NewTicker(time.Duration(duration) * time.Minute)
	for {
		<-ticker.C
		for _, id := range m.cfg.ClusterMembership.IDs {
			start, end := m.GetFileIdxes(id)
			m.CopyFileFromRemoteToLocal(id, start, end, m.cfg.HistoryDir)
			m.DeleteFile(id, start, end)
		}
	}
}
