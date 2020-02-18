package pprof

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/gojek/kafqa/logger"
)

func StartServer(port int) {
	router := http.NewServeMux()
	logger.Infof("Setting up PProf on port %d", port)
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), router)
		if err != nil {
			logger.Errorf("Error Starting pprof endpoint %v", err)
		}
	}()

}
