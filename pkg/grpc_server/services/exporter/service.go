package exporter

import (
	"bytes"
	"crypto/tls"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/net/context"

	pb "github.com/BrobridgeOrg/gravity-api/service/exporter"
	app "github.com/BrobridgeOrg/gravity-exporter-rest/pkg/app"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var counter uint64 = 0
var transport = &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

type Event struct {
	Channel string
	Payload []byte
}

var eventPool = sync.Pool{
	New: func() interface{} {
		return &Event{}
	},
}

type Service struct {
	app      app.App
	incoming chan *Event
}

func NewService(a app.App) *Service {

	service := &Service{
		app:      a,
		incoming: make(chan *Event, 204800),
	}

	go service.eventHandler()

	return service
}

func (service *Service) eventHandler() {

	// Getting parameters
	method := strings.ToUpper(viper.GetString("rest.method"))
	uri := viper.GetString("rest.uri")
	headers := viper.GetStringMapString("rest.headers")
	for {
		select {
		case event := <-service.incoming:

			// Getting parameters
			payload := bytes.NewReader([]byte(event.Payload))

			// Create a request
			request, err := http.NewRequest(method, uri, payload)
			if err != nil {
				log.Error(err)
			}

			// Preparing header
			request.Header.Add("Gravity-Version", "1.0")
			request.Header.Add("Content-Type", "application/json")
			for key, value := range headers {
				request.Header.Add(key, value)
			}

			client := http.Client{
				Transport: transport,
			}
			resp, err := client.Do(request)
			if err != nil {
				log.Error(err)
			}

			// Require body
			defer resp.Body.Close()

			// Discard body
			//_, err = io.Copy(ioutil.Discard, resp.Body)
			_, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Error(err)
			}

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				if err != nil {
					log.Error("Target HTTP server return with error status code:", resp.StatusCode)
				}
			}
		}
	}
}

func (service *Service) SendEvent(ctx context.Context, in *pb.SendEventRequest) (*pb.SendEventReply, error) {

	id := atomic.AddUint64((*uint64)(&counter), 1)
	if id%1000 == 0 {
		log.Info(id)
	}

	// Getting parameters
	method := strings.ToUpper(viper.GetString("rest.method"))
	uri := viper.GetString("rest.uri")
	headers := viper.GetStringMapString("rest.headers")
	payload := bytes.NewReader([]byte(in.Payload))

	// Create a request
	request, err := http.NewRequest(method, uri, payload)
	if err != nil {
		return &pb.SendEventReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	// Preparing header
	request.Header.Add("Gravity-Version", "1.0")
	request.Header.Add("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Add(key, value)
	}

	client := http.Client{
		Transport: transport,
	}
	resp, err := client.Do(request)
	if err != nil {
		return &pb.SendEventReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	// Require body
	defer resp.Body.Close()

	// Discard body
	//_, err = io.Copy(ioutil.Discard, resp.Body)
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return &pb.SendEventReply{
			Success: false,
			Reason:  err.Error(),
		}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &pb.SendEventReply{
			Success: false,
			Reason:  "Target HTTP server return with error status code",
		}, nil
	}

	return &pb.SendEventReply{
		Success: true,
	}, nil
}

func (service *Service) SendEventStream(stream pb.Exporter_SendEventStreamServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		id := atomic.AddUint64((*uint64)(&counter), 1)
		if id%1000 == 0 {
			log.Info(id)
		}

		event := eventPool.Get().(*Event)
		event.Channel = in.Channel
		event.Payload = in.Payload

		service.incoming <- event
	}
}
