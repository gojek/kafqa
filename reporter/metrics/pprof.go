package metrics

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/logger"
)

func SetupPProf(cfg config.PProf) {
	if cfg.Enabled {
		router := http.NewServeMux()
		logger.Infof("Setting up PProf %v", cfg.Enabled)
		router.HandleFunc("/debug/pprof/", pprof.Index)
		router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		router.HandleFunc("/debug/pprof/profile", pprof.Profile)
		router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		router.HandleFunc("/debug/pprof/trace", pprof.Trace)

		go func() {
			err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), router)
			if err != nil {
				logger.Errorf("Error Starting pprof endpoint %v", err)
			}
		}()
	} else {
		logger.Infof("PProf is not running")
	}
}
