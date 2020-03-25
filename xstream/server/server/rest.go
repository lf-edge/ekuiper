package server

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/plugins"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

const (
	ContentType     = "Content-Type"
	ContentTypeJSON = "application/json"
)

type statementDescriptor struct {
	Sql string `json:"sql,omitempty"`
}

func decodeStatementDescriptor(reader io.ReadCloser) (statementDescriptor, error) {
	sd := statementDescriptor{}
	err := json.NewDecoder(reader).Decode(&sd)
	// Problems decoding
	if err != nil {
		return sd, fmt.Errorf("Error decoding the statement descriptor: %v", err)
	}
	return sd, nil
}

// Handle applies the specified error and error concept tot he HTTP response writer
func handleError(w http.ResponseWriter, err error, ec int, logger api.Logger) {
	message := err.Error()
	logger.Error(message)
	http.Error(w, message, ec)
}

func jsonResponse(i interface{}, w http.ResponseWriter, logger api.Logger) {
	w.Header().Add(ContentType, ContentTypeJSON)
	enc := json.NewEncoder(w)
	err := enc.Encode(i)
	// Problems encoding
	if err != nil {
		handleError(w, err, http.StatusBadRequest, logger)
		return
	}
}

func createRestServer(port int) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/", rootHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/streams", streamsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/streams/{name}", streamHandler).Methods(http.MethodGet, http.MethodDelete)
	r.HandleFunc("/rules", rulesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/rules/{name}", ruleHandler).Methods(http.MethodDelete, http.MethodGet)
	r.HandleFunc("/rules/{name}/status", getStatusRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/start", startRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/stop", stopRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/restart", restartRuleHandler).Methods(http.MethodPost)

	r.HandleFunc("/plugins/sources", sourcesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/sources/{name}", sourceHandler).Methods(http.MethodDelete)
	r.HandleFunc("/plugins/sinks", sinksHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/sinks/{name}", sinkHandler).Methods(http.MethodDelete)
	r.HandleFunc("/plugins/functions", functionsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/functions/{name}", functionHandler).Methods(http.MethodDelete)

	server := &http.Server{
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r, // Pass our instance of gorilla/mux in.
	}
	server.SetKeepAlivesEnabled(false)
	return server
}

//The handler for root
func rootHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet, http.MethodPost:
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	}
}

//list or create streams
func streamsHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content, err := streamProcessor.ShowStream()
		if err != nil {
			handleError(w, fmt.Errorf("Stream command error: %s", err), http.StatusBadRequest, logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodPost:
		v, err := decodeStatementDescriptor(r.Body)
		if err != nil {
			handleError(w, fmt.Errorf("Invalid body: %s", err), http.StatusBadRequest, logger)
			return
		}
		content, err := streamProcessor.ExecStreamSql(v.Sql)
		if err != nil {
			handleError(w, fmt.Errorf("Stream command error: %s", err), http.StatusBadRequest, logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(content))
	}
}

//describe or delete a stream
func streamHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	switch r.Method {
	case http.MethodGet:
		content, err := streamProcessor.DescStream(name)
		if err != nil {
			handleError(w, fmt.Errorf("describe stream error: %s", err), http.StatusBadRequest, logger)
			return
		}
		//TODO format data type
		jsonResponse(content, w, logger)
	case http.MethodDelete:
		content, err := streamProcessor.DropStream(name)
		if err != nil {
			handleError(w, fmt.Errorf("describe stream error: %s", err), http.StatusBadRequest, logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(content))
	}
}

//list or create rules
func rulesHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPost:
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error reading body: %v", err)
			handleError(w, fmt.Errorf("Invalid body: %s", err), http.StatusBadRequest, logger)
			return
		}
		r, err := ruleProcessor.ExecCreate("", string(body))
		var result string
		if err != nil {
			handleError(w, fmt.Errorf("Create rule error : %s.", err), http.StatusBadRequest, logger)
			return
		} else {
			result = fmt.Sprintf("Rule %s was created, please use 'cli getstatus rule $rule_name' command to get rule status.", r.Id)
		}
		//Start the rule
		rs, err := createRuleState(r)
		if err != nil {
			result = err.Error()
		} else {
			err = doStartRule(rs)
			if err != nil {
				result = err.Error()
			}
		}

		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(result))
	case http.MethodGet:
		content, err := ruleProcessor.GetAllRules()
		if err != nil {
			handleError(w, fmt.Errorf("Show rules error: %s", err), http.StatusBadRequest, logger)
			return
		}
		jsonResponse(content, w, logger)
	}
}

//describe or delete a rule
func ruleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	switch r.Method {
	case http.MethodGet:
		rule, err := ruleProcessor.GetRuleByName(name)
		if err != nil {
			handleError(w, fmt.Errorf("describe stream error: %s", err), http.StatusBadRequest, logger)
			return
		}
		jsonResponse(rule, w, logger)
	case http.MethodDelete:
		stopRule(name)
		content, err := ruleProcessor.ExecDrop(name)
		if err != nil {
			handleError(w, fmt.Errorf("drop rule error: %s", err), http.StatusBadRequest, logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(content))
	}
}

//get status of a rule
func getStatusRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	content, err := getRuleStatus(name)
	if err != nil {
		handleError(w, fmt.Errorf("get rule status error: %s", err), http.StatusBadRequest, logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(content))
}

//start a rule
func startRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	err := startRule(name)
	if err != nil {
		handleError(w, fmt.Errorf("start rule error: %s", err), http.StatusBadRequest, logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Rule %s was started", name)))
}

//stop a rule
func stopRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	result := stopRule(name)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(result))
}

//restart a rule
func restartRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	err := restartRule(name)
	if err != nil {
		handleError(w, fmt.Errorf("restart rule error: %s", err), http.StatusBadRequest, logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Rule %s was restarted", name)))
}

func pluginsHandler(w http.ResponseWriter, r *http.Request, t plugins.PluginType) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content, err := pluginManager.List(t)
		if err != nil {
			handleError(w, fmt.Errorf("%s plugins list command error: %s", plugins.PluginTypes[t], err), http.StatusBadRequest, logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodPost:
		sd := plugins.Plugin{}
		err := json.NewDecoder(r.Body).Decode(&sd)
		// Problems decoding
		if err != nil {
			handleError(w, fmt.Errorf("Invalid body: Error decoding the %s plugin json: %v", plugins.PluginTypes[t], err), http.StatusBadRequest, logger)
			return
		}
		err = pluginManager.Register(t, &sd)
		if err != nil {
			handleError(w, fmt.Errorf("%s plugins create command error: %s", plugins.PluginTypes[t], err), http.StatusBadRequest, logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(fmt.Sprintf("%s plugin %s is created", plugins.PluginTypes[t], sd.Name)))
	}
}

func pluginHandler(w http.ResponseWriter, r *http.Request, t plugins.PluginType) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	cb := r.URL.Query().Get("callback")

	switch r.Method {
	case http.MethodDelete:
		err := pluginManager.Delete(t, name, cb)
		if err != nil {
			handleError(w, fmt.Errorf("delete %s plugin %s error: %s", plugins.PluginTypes[t], name, err), http.StatusBadRequest, logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf("%s plugin %s is deleted", plugins.PluginTypes[t], name)))
	}
}

//list or create source plugin
func sourcesHandler(w http.ResponseWriter, r *http.Request) {
	pluginsHandler(w, r, plugins.SOURCE)
}

//delete a source plugin
func sourceHandler(w http.ResponseWriter, r *http.Request) {
	pluginHandler(w, r, plugins.SOURCE)
}

//list or create sink plugin
func sinksHandler(w http.ResponseWriter, r *http.Request) {
	pluginsHandler(w, r, plugins.SINK)
}

//delete a sink plugin
func sinkHandler(w http.ResponseWriter, r *http.Request) {
	pluginHandler(w, r, plugins.SINK)
}

//list or create function plugin
func functionsHandler(w http.ResponseWriter, r *http.Request) {
	pluginsHandler(w, r, plugins.FUNCTION)
}

//delete a function plugin
func functionHandler(w http.ResponseWriter, r *http.Request) {
	pluginHandler(w, r, plugins.FUNCTION)
}
