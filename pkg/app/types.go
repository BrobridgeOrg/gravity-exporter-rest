package app

import (
	"github.com/BrobridgeOrg/gravity-exporter-rest/pkg/grpc_server"
	"github.com/BrobridgeOrg/gravity-exporter-rest/pkg/mux_manager"
)

type App interface {
	GetGRPCServer() grpc_server.Server
	GetMuxManager() mux_manager.Manager
}
