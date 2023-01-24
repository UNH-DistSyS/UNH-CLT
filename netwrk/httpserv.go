package netwrk

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
	"github.com/UNH-DistSyS/UNH-CLT/request"
)

type HttpServ struct {
	ids.ID        // must have an identity
	httpAddresses map[ids.ID]string
	operation_dispatcher.ConcurrentOperationDispatcher
	serv *http.Server
}

func NewHttpServ(identity ids.ID, cfg *config.Config) *HttpServ {
	log.Infof("Creating HttpServ on node %v", identity)
	serv := new(HttpServ)
	serv.ID = identity
	serv.httpAddresses = cfg.ClusterMembership.HTTPAddrs
	serv.ConcurrentOperationDispatcher = *operation_dispatcher.NewConcurrentOperationDispatcher(serv.ID, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = cfg.MaxIdleHttpCnx
	return serv
}

func (n *HttpServ) serveRequest(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorln("error reading request body: ", err)
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}

	r.Context()

	api := r.Header.Get(request.HTTPHeaderAPI)
	switch api {
	case "kv":
		var kvReq request.KVRequest
		err = json.Unmarshal(body, &kvReq)
		log.Debugf("Serving KV Http request: %+v", kvReq)
		if err != nil {
			log.Errorln("error reading JSON in request body: ", err)
			http.Error(w, "error reading JSON in request body", http.StatusBadRequest)
			return
		}
		kvReq.C = make(chan request.KVReply, 1)
		n.EnqueueOperation(r.Context(), kvReq)
		kvReply := <-kvReq.C
		replyData, err := json.Marshal(kvReply)
		if err != nil {
			log.Errorf("Error encoding a response to client_driver: %s", err)
		}
		_, err = w.Write(replyData)
		if err != nil {
			log.Errorf("Error writing a response to client_driver: %s", err)
		}
	case "cql":
		var cqlReq request.CqlRequest
		err = json.Unmarshal(body, &cqlReq)
		log.Debugf("Serving CQL Http request: %+v", cqlReq)
		if err != nil {
			log.Errorln("error reading JSON in request body: ", err)
			http.Error(w, "error reading JSON in request body", http.StatusBadRequest)
			return
		}
		cqlReq.C = make(chan request.CqlReply, 1)
		n.EnqueueOperation(r.Context(), cqlReq)
		cqlReply := <-cqlReq.C
		replyData, err := json.Marshal(cqlReply)
		if err != nil {
			log.Errorf("Error encoding a response to client_driver: %s", err)
		}
		_, err = w.Write(replyData)
		if err != nil {
			log.Errorf("Error writing a response to client_driver: %s", err)
		}
	}
}

// serve serves the http REST API request from clients
func (n *HttpServ) serve() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.serveRequest)

	// http string should be in form of ":8080"
	url, _ := url.Parse(n.httpAddresses[n.ID])
	port := ":" + url.Port()
	n.serv = &http.Server{
		Addr:    port,
		Handler: mux,
	}
	log.Debugf("About to listen on port %v", port)
	err := n.serv.ListenAndServe()
	if err != nil {
		log.Errorf("HttpServer error: %v", err)
	}
}

// Run start and run the node
func (n *HttpServ) Run() {
	log.Infof("Starting HttpServ at node %v\n", n.ID)
	n.ConcurrentOperationDispatcher.Run()
	go n.serve()
}

func (n *HttpServ) Close() {
	log.Infof("Closing HttpServ at node %v", n.ID)
	ctx := context.TODO()
	err := n.serv.Shutdown(ctx)
	if err != nil {
		log.Errorf("Error closing http node_assembler: %v", err)
	}
}
