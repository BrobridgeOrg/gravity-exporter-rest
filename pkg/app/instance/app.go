package instance

import (
	grpc_server "github.com/BrobridgeOrg/gravity-exporter-rest/pkg/grpc_server/server"
	mux_manager "github.com/BrobridgeOrg/gravity-exporter-rest/pkg/mux_manager/manager"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done       chan bool
	muxManager *mux_manager.MuxManager
	grpcServer *grpc_server.Server
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	return a
}

func (a *AppInstance) Init() error {

	log.Info("Starting application")

	// Initializing modules
	a.muxManager = mux_manager.NewMuxManager(a)
	a.grpcServer = grpc_server.NewServer(a)

	a.initMuxManager()

	// Initializing GRPC server
	err := a.initGRPCServer()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	// GRPC
	go func() {
		err := a.runGRPCServer()
		if err != nil {
			log.Error(err)
		}
	}()

	err := a.runMuxManager()
	if err != nil {
		return err
	}

	<-a.done

	return nil
}
