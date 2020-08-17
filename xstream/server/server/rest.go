package server

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/plugins"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"strings"
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
func handleError(w http.ResponseWriter, err error, prefix string, logger api.Logger) {
	message := prefix
	if message != "" {
		message += ": "
	}
	message += err.Error()
	logger.Error(message)
	var ec int
	switch e := err.(type) {
	case *common.Error:
		switch e.Code() {
		case common.NOT_FOUND:
			ec = http.StatusNotFound
		default:
			ec = http.StatusBadRequest
		}
	default:
		ec = http.StatusBadRequest
	}
	http.Error(w, message, ec)
}

func jsonResponse(i interface{}, w http.ResponseWriter, logger api.Logger) {
	w.Header().Add(ContentType, ContentTypeJSON)
	enc := json.NewEncoder(w)
	err := enc.Encode(i)
	// Problems encoding
	if err != nil {
		handleError(w, err, "", logger)
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
	r.HandleFunc("/plugins/sources/{name}", sourceHandler).Methods(http.MethodDelete, http.MethodGet)
	r.HandleFunc("/plugins/sinks", sinksHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/sinks/{name}", sinkHandler).Methods(http.MethodDelete, http.MethodGet)
	r.HandleFunc("/plugins/functions", functionsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/functions/{name}", functionHandler).Methods(http.MethodDelete, http.MethodGet)

	r.HandleFunc("/metadata/sinks", metadataHandler).Methods(http.MethodGet)

	server := &http.Server{
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      handlers.CORS(handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Type", "Content-Language", "Origin"}))(r),
	}
	server.SetKeepAlivesEnabled(false)
	return server
}

type information struct {
	Version       string `json:"version"`
	Os            string `json:"os"`
	UpTimeSeconds int64  `json:"upTimeSeconds"`
}

//The handler for root
func rootHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet, http.MethodPost:
		w.WriteHeader(http.StatusOK)
		info := new(information)
		info.Version = version
		info.UpTimeSeconds = time.Now().Unix() - startTimeStamp
		info.Os = runtime.GOOS
		byteInfo, _ := json.Marshal(info)
		w.Write(byteInfo)
	}
}

//list or create streams
func streamsHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content, err := streamProcessor.ShowStream()
		if err != nil {
			handleError(w, err, "Stream command error", logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodPost:
		v, err := decodeStatementDescriptor(r.Body)
		if err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		content, err := streamProcessor.ExecStreamSql(v.Sql)
		if err != nil {
			handleError(w, err, "Stream command error", logger)
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
			handleError(w, err, "describe stream error", logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodDelete:
		content, err := streamProcessor.DropStream(name)
		if err != nil {
			handleError(w, err, "delete stream error", logger)
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
			handleError(w, err, "Invalid body", logger)
			return
		}
		r, err := ruleProcessor.ExecCreate("", string(body))
		var result string
		if err != nil {
			handleError(w, err, "Create rule error", logger)
			return
		} else {
			result = fmt.Sprintf("Rule %s was created successfully.", r.Id)
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
		content, err := getAllRulesWithStatus()
		if err != nil {
			handleError(w, err, "Show rules error", logger)
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
			handleError(w, err, "describe rule error", logger)
			return
		}
		jsonResponse(rule, w, logger)
	case http.MethodDelete:
		stopRule(name)
		content, err := ruleProcessor.ExecDrop(name)
		if err != nil {
			handleError(w, err, "delete rule error", logger)
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
		handleError(w, err, "get rule status error", logger)
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
		handleError(w, err, "start rule error", logger)
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
		handleError(w, err, "restart rule error", logger)
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
			handleError(w, err, fmt.Sprintf("%s plugins list command error", plugins.PluginTypes[t]), logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodPost:
		sd := plugins.Plugin{}
		err := json.NewDecoder(r.Body).Decode(&sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, fmt.Sprintf("Invalid body: Error decoding the %s plugin json", plugins.PluginTypes[t]), logger)
			return
		}
		err = pluginManager.Register(t, &sd)
		if err != nil {
			handleError(w, err, fmt.Sprintf("%s plugins create command error", plugins.PluginTypes[t]), logger)
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
	cb := r.URL.Query().Get("stop")

	switch r.Method {
	case http.MethodDelete:
		r := cb == "1"
		err := pluginManager.Delete(t, name, r)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete %s plugin %s error", plugins.PluginTypes[t], name), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		result := fmt.Sprintf("%s plugin %s is deleted", plugins.PluginTypes[t], name)
		if r {
			result = fmt.Sprintf("%s and Kuiper will be stopped", result)
		} else {
			result = fmt.Sprintf("%s and Kuiper must restart for the change to take effect.", result)
		}
		w.Write([]byte(result))
	case http.MethodGet:
		j, ok := pluginManager.Get(t, name)
		if !ok {
			handleError(w, common.NewErrorWithCode(common.NOT_FOUND, "not found"), fmt.Sprintf("describe %s plugin %s error", plugins.PluginTypes[t], name), logger)
			return
		}
		jsonResponse(j, w, logger)
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

func parseRequest(req string) map[string]string {
	mapQuery := make(map[string]string)
	for _, kv := range strings.Split(req, "&") {
		pos := strings.Index(kv, "=")
		if 0 < pos && pos+1 < len(kv) {
			mapQuery[kv[:pos]], _ = url.QueryUnescape(kv[pos+1:])
		}
	}
	return mapQuery
}

func metadataHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if 0 == len(r.URL.RawQuery) {
		sinks := pluginManager.GetSinks()
		jsonResponse(sinks, w, logger)
		return
	}

	mapQuery := parseRequest(r.URL.RawQuery)
	ruleid := mapQuery["rule"]
	pluginName := mapQuery["name"]

	var rule *api.Rule
	var err error
	if 0 != len(ruleid) {
		rule, err = ruleProcessor.GetRuleByName(ruleid)
		if err != nil {
			handleError(w, err, "describe rule error", logger)
			return
		}
	}

	ptrMetadata, err := pluginManager.Metadata(pluginName, rule)

	if err != nil {
		handleError(w, err, "metadata error", logger)
		return
	}
	jsonResponse(ptrMetadata, w, logger)
}
