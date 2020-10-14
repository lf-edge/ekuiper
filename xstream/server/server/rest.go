package server

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/plugins"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"golang.org/x/net/html"
	"io"
	"io/ioutil"
	"net/http"
	"os"
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
	r.HandleFunc("/rules/{name}/topo", getTopoRuleHandler).Methods(http.MethodGet)

	r.HandleFunc("/plugins/sources", sourcesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/sources/prebuild", prebuildSourcePlugins).Methods(http.MethodGet)
	r.HandleFunc("/plugins/sources/{name}", sourceHandler).Methods(http.MethodDelete, http.MethodGet)

	r.HandleFunc("/plugins/sinks", sinksHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/sinks/prebuild", prebuildSinkPlugins).Methods(http.MethodGet)
	r.HandleFunc("/plugins/sinks/{name}", sinkHandler).Methods(http.MethodDelete, http.MethodGet)
	r.HandleFunc("/plugins/functions", functionsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/functions/prebuild", prebuildFuncsPlugins).Methods(http.MethodGet)
	r.HandleFunc("/plugins/functions/{name}", functionHandler).Methods(http.MethodDelete, http.MethodGet)

	r.HandleFunc("/metadata/functions", functionsMetaHandler).Methods(http.MethodGet)

	r.HandleFunc("/metadata/sinks", sinksMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sinks/{name}", newSinkMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sinks/rule/{id}", showSinkMetaHandler).Methods(http.MethodGet)

	r.HandleFunc("/metadata/sources", sourcesMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources/yaml/{name}", sourceConfHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources/{name}", sourceMetaHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources/{name}/confKeys", sourceConfKeysHandler).Methods(http.MethodGet)
	r.HandleFunc("/metadata/sources/{name}/confKeys/{confKey}", sourceConfKeyHandler).Methods(http.MethodDelete, http.MethodPost)
	r.HandleFunc("/metadata/sources/{name}/confKeys/{confKey}/field", sourceConfKeyFieldsHandler).Methods(http.MethodDelete, http.MethodPost)

	server := &http.Server{
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 60 * 5,
		ReadTimeout:  time.Second * 60 * 5,
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

//get topo of a rule
func getTopoRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	content, err := getRuleTopo(name)
	if err != nil {
		handleError(w, err, "get rule topo error", logger)
		return
	}
	w.Header().Set(ContentType, ContentTypeJSON)
	w.Write([]byte(content))
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

func prebuildSourcePlugins(w http.ResponseWriter, r *http.Request) {
	prebuildPluginsHandler(w, r, plugins.SOURCE)
}

func prebuildSinkPlugins(w http.ResponseWriter, r *http.Request) {
	prebuildPluginsHandler(w, r, plugins.SINK)
}

func prebuildFuncsPlugins(w http.ResponseWriter, r *http.Request) {
	prebuildPluginsHandler(w, r, plugins.FUNCTION)
}

func isOffcialDockerImage() bool {
	if strings.ToLower(os.Getenv("MAINTAINER")) != "emqx.io" {
		return false
	}
	return true
}

func prebuildPluginsHandler(w http.ResponseWriter, r *http.Request, t plugins.PluginType) {
	emsg := "It's strongly recommended to install plugins at official released Debian Docker images. If you choose to proceed to install plugin, please make sure the plugin is already validated in your own build."
	if !isOffcialDockerImage() {
		handleError(w, fmt.Errorf(emsg), "", logger)
		return
	} else if runtime.GOOS == "linux" {
		osrelease, err := common.Read()
		if err != nil {
			logger.Infof("")
			return
		}
		prettyName := strings.ToUpper(osrelease["PRETTY_NAME"])
		os := "debian"
		if strings.Contains(prettyName, "DEBIAN") {
			hosts := common.Config.Basic.PluginHosts
			ptype := "sources"
			if t == plugins.SINK {
				ptype = "sinks"
			} else if t == plugins.FUNCTION {
				ptype = "functions"
			}
			if err, plugins := fetchPluginList(hosts, ptype, os, runtime.GOARCH); err != nil {
				handleError(w, err, "", logger)
			} else {
				jsonResponse(plugins, w, logger)
			}
		} else {
			handleError(w, fmt.Errorf(emsg), "", logger)
			return
		}
	} else {
		handleError(w, fmt.Errorf(emsg), "", logger)
	}
}

func fetchPluginList(hosts, ptype, os, arch string) (err error, result map[string]string) {
	if hosts == "" || ptype == "" || os == "" {
		logger.Errorf("Invalid parameter value: hosts %s, ptype %s or os: %s should not be empty.", hosts, ptype, os)
		return fmt.Errorf("Invalid configruation for plugin host in kuiper.yaml."), nil
	}
	result = make(map[string]string)
	hostsArr := strings.Split(hosts, ",")
	for _, host := range hostsArr {
		host := strings.Trim(host, " ")
		tmp := []string{host, "kuiper-plugins", version, os, ptype}
		//The url is similar to http://host:port/kuiper-plugins/0.9.1/debian/sinks/
		url := strings.Join(tmp, "/")
		timeout := time.Duration(30 * time.Second)
		client := &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
		resp, err := client.Get(url)
		logger.Infof("Trying to fetch plugins from url: %s\n", url)

		if err != nil {
			return err, nil
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("Cannot fetch plugin list from %s, with status error: %v", url, resp.StatusCode), nil
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err, nil
		}
		plugins := extractFromHtml(string(data), arch)
		for _, p := range plugins {
			//If already existed, using the existed.
			if _, ok := result[p]; !ok {
				result[p] = url + "/" + p + "_" + arch + ".zip"
			}
			logger.Debugf("Plugin %s, download address is %s\n", p, result[p])
		}
	}
	return
}

func extractFromHtml(content, arch string) []string {
	plugins := []string{}
	htmlTokens := html.NewTokenizer(strings.NewReader(content))
loop:
	for {
		tt := htmlTokens.Next()
		switch tt {
		case html.ErrorToken:
			break loop
		case html.StartTagToken:
			t := htmlTokens.Token()
			isAnchor := t.Data == "a"
			if isAnchor {
				found := false
				for _, prop := range t.Attr {
					if strings.ToUpper(prop.Key) == "HREF" {
						if strings.HasSuffix(prop.Val, "_"+arch+".zip") {
							if index := strings.LastIndex(prop.Val, "_"); index != -1 {
								plugins = append(plugins, prop.Val[0:index])
							}
						}
						found = true
					}
				}
				if !found {
					logger.Infof("Invalid plugin download link %s", t)
				}
			}
		}
	}
	return plugins
}

//list sink plugin
func sinksMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	sinks := plugins.GetSinks()
	jsonResponse(sinks, w, logger)
	return
}

//Get sink metadata when creating rules
func newSinkMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]

	v := r.URL.Query()
	language := v.Get("language")
	if 0 == len(language) {
		language = "en_US"
	}
	ptrMetadata, err := plugins.GetSinkMeta(pluginName, language, nil)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	jsonResponse(ptrMetadata, w, logger)
}

//Get sink metadata when displaying rules
func showSinkMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	ruleid := vars["id"]

	rule, err := ruleProcessor.GetRuleByName(ruleid)
	if err != nil {
		handleError(w, err, "describe rule error", logger)
		return
	}

	v := r.URL.Query()
	language := v.Get("language")
	if 0 == len(language) {
		language = "en_US"
	}
	ptrMetadata, err := plugins.GetSinkMeta("", language, rule)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	jsonResponse(ptrMetadata, w, logger)
}

//list functions
func functionsMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	sinks := plugins.GetFunctions()
	jsonResponse(sinks, w, logger)
	return
}

//list source plugin
func sourcesMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ret := plugins.GetSources()
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}

//Get source metadata when creating stream
func sourceMetaHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]
	v := r.URL.Query()
	language := v.Get("language")
	if 0 == len(language) {
		language = "en_US"
	}
	ret, err := plugins.GetSourceMeta(pluginName, language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}

//Get source yaml
func sourceConfHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]
	v := r.URL.Query()
	language := v.Get("language")
	if 0 == len(language) {
		language = "en_US"
	}
	ret, err := plugins.GetSourceConf(pluginName, language)
	if err != nil {
		handleError(w, err, "", logger)
		return
	} else {
		w.Write(ret)
	}
}

//Get confKeys of the source metadata
func sourceConfKeysHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	pluginName := vars["name"]
	ret := plugins.GetSourceConfKeys(pluginName)
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}

//Add  del confkey
func sourceConfKeyHandler(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()
	var ret interface{}
	var err error
	vars := mux.Vars(r)
	pluginName := vars["name"]
	confKey := vars["confKey"]

	v := r.URL.Query()
	language := v.Get("language")
	if 0 == len(language) {
		language = "en_US"
	}

	switch r.Method {
	case http.MethodDelete:
		err = plugins.DelSourceConfKey(pluginName, confKey, language)
	case http.MethodPost:
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		err = plugins.AddSourceConfKey(pluginName, confKey, language, v)
	}
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}

//Del and Update field of confkey
func sourceConfKeyFieldsHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var ret interface{}
	var err error
	vars := mux.Vars(r)
	pluginName := vars["name"]
	confKey := vars["confKey"]
	v, err := ioutil.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, "Invalid body", logger)
		return
	}

	val := r.URL.Query()
	language := val.Get("language")
	if 0 == len(language) {
		language = "en_US"
	}

	switch r.Method {
	case http.MethodDelete:
		err = plugins.DelSourceConfKeyField(pluginName, confKey, language, v)
	case http.MethodPost:
		err = plugins.AddSourceConfKeyField(pluginName, confKey, language, v)
	}
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	if nil != ret {
		jsonResponse(ret, w, logger)
		return
	}
}
