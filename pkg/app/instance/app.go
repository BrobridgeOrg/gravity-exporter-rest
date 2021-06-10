package instance

import (
	"runtime"

	subscriber "github.com/BrobridgeOrg/gravity-exporter-rest/pkg/subscriber/service"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done       chan bool
	subscriber *subscriber.Subscriber
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	return a
}

func (a *AppInstance) Init() error {

	log.WithFields(log.Fields{
		"max_procs": runtime.GOMAXPROCS(0),
	}).Info("Starting application")

	// Initializing modules
	a.subscriber = subscriber.NewSubscriber(a)

	err := a.subscriber.Init()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	err := a.subscriber.Run()
	if err != nil {
		return err
	}

	<-a.done

	return nil
}
