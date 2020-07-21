package server

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/plugins"
	"github.com/emqx/kuiper/xsql/processors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"context"
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"
)

var (
	dataDir         string
	logger          = common.Log
	startTimeStamp  int64
	version         = ""
	ruleProcessor   *processors.RuleProcessor
	streamProcessor *processors.StreamProcessor
	pluginManager   *plugins.Manager
)

func StartUp(Version, LoadFileType string) {
	version = Version
	common.LoadFileType = LoadFileType
	startTimeStamp = time.Now().Unix()
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
	pluginManager, err = plugins.NewPluginManager()
	if err != nil {
		logger.Panic(err)
	}

	registry = &RuleRegistry{internal: make(map[string]*RuleState)}

	server := new(Server)
	//Start rules
	if rules, err := ruleProcessor.GetAllRules(); err != nil {
		logger.Infof("Start rules error: %s", err)
	} else {
		logger.Info("Starting rules")
		var reply string
		for _, rule := range rules {
			//err = server.StartRule(rule, &reply)
			reply = recoverRule(rule)
			if 0 != len(reply) {
				logger.Info(reply)
			}
		}
	}

	//Start prometheus service
	var srvPrometheus *http.Server = nil
	if common.Config.Basic.Prometheus {
		portPrometheus := common.Config.Basic.PrometheusPort
		if portPrometheus <= 0 {
			logger.Fatal("Miss configuration prometheusPort")
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srvPrometheus = &http.Server{
			Addr:         fmt.Sprintf("0.0.0.0:%d", portPrometheus),
			WriteTimeout: time.Second * 15,
			ReadTimeout:  time.Second * 15,
			IdleTimeout:  time.Second * 60,
			Handler:      mux,
		}
		go func() {
			if err := srvPrometheus.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatal("Listen prometheus error: ", err)
			}
		}()
		msg := fmt.Sprintf("Serving prometheus metrics on port http://localhost:%d/metrics", portPrometheus)
		logger.Infof(msg)
		fmt.Println(msg)
	}

	//Start rest service
	srvRest := createRestServer(common.Config.Basic.RestPort)
	go func() {
		var err error
		if common.Config.Basic.RestTls == nil {
			err = srvRest.ListenAndServe()
		} else {
			err = srvRest.ListenAndServeTLS(common.Config.Basic.RestTls.Certfile, common.Config.Basic.RestTls.Keyfile)
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Fatal("Error serving rest service: ", err)
		}
	}()

	// Start rpc service
	portRpc := common.Config.Basic.Port
	rpcSrv := rpc.NewServer()
	err = rpcSrv.Register(server)
	if err != nil {
		logger.Fatal("Format of service Server isn'restHttpType correct. ", err)
	}
	srvRpc := &http.Server{
		Addr:         fmt.Sprintf("0.0.0.0:%d", portRpc),
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      rpcSrv,
	}
	go func() {
		if err = srvRpc.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Error serving rpc service:", err)
		}
	}()

	//Startup message
	restHttpType := "http"
	if common.Config.Basic.RestTls != nil {
		restHttpType = "https"
	}
	msg := fmt.Sprintf("Serving kuiper (version - %s) on port %d, and restful api on %s://0.0.0.0:%d. \n", Version, common.Config.Basic.Port, restHttpType, common.Config.Basic.RestPort)
	logger.Info(msg)
	fmt.Printf(msg)

	//Stop the services
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
	<-sigint

	if err = srvRpc.Shutdown(context.TODO()); err != nil {
		logger.Error("rpc server shutdown error: %v", err)
	}
	logger.Info("rpc server shutdown.")

	if err = srvRest.Shutdown(context.TODO()); err != nil {
		logger.Error("rest server shutdown error: %v", err)
	}
	logger.Info("rest server successfully shutdown.")

	if err = srvPrometheus.Shutdown(context.TODO()); err != nil {
		logger.Error("prometheus server shutdown error: %v", err)
	}
	logger.Info("prometheus server successfully shutdown.")

	os.Exit(0)
}
