package server

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql/processors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net"
	"net/http"
	"net/rpc"
	"path"
)

var (
	dataDir string
	logger  = common.Log

	ruleProcessor   *processors.RuleProcessor
	streamProcessor *processors.StreamProcessor
)

func StartUp(Version string) {
	common.InitConf()

	dr, err := common.GetDataLoc()
	if err != nil {
		logger.Panic(err)
	} else {
		logger.Infof("db location is %s", dr)
		dataDir = dr
	}
	ruleProcessor = processors.NewRuleProcessor(path.Dir(dataDir))
	streamProcessor = processors.NewStreamProcessor(path.Join(path.Dir(dataDir), "stream"))

	registry = &RuleRegistry{internal: make(map[string]*RuleState)}

	server := new(Server)
	//Start rules
	if rules, err := ruleProcessor.GetAllRules(); err != nil {
		logger.Infof("Start rules error: %s", err)
	} else {
		logger.Info("Starting rules")
		var reply string
		for _, rule := range rules {
			err = server.StartRule(rule, &reply)
			if err != nil {
				logger.Info(err)
			} else {
				logger.Info(reply)
			}
		}
	}

	//Start server
	err = rpc.Register(server)
	if err != nil {
		logger.Fatal("Format of service Server isn't correct. ", err)
	}
	// Register a HTTP handler
	rpc.HandleHTTP()
	// Listen to TPC connections on port 1234
	listener, e := net.Listen("tcp", fmt.Sprintf(":%d", common.Config.Port))
	if e != nil {
		logger.Fatal("Listen error: ", e)
	}
	msg := fmt.Sprintf("Serving kuiper (version - %s) on port %d... \n", Version, common.Config.Port)
	logger.Info(msg)
	fmt.Printf(msg)
	if common.Config.Prometheus {
		go func() {
			port := common.Config.PrometheusPort
			if port <= 0 {
				logger.Fatal("Miss configuration prometheusPort")
			}
			listener, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
			if e != nil {
				logger.Fatal("Listen prometheus error: ", e)
			}
			logger.Infof("Serving prometheus metrics on port http://localhost:%d/metrics", port)
			http.Handle("/metrics", promhttp.Handler())
			http.Serve(listener, nil)
		}()
	}

	//Start rest service
	srv := createRestServer(common.Config.RestPort)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			logger.Fatal("Error serving rest service: ", err)
		}
	}()

	// Start accept incoming HTTP connections
	err = http.Serve(listener, nil)
	if err != nil {
		logger.Fatal("Error serving: ", err)
	}
}
