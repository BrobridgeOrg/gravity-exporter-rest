package exporter

import (
	"bytes"
	"crypto/tls"
	"io/ioutil"
	"net/http"
	"strings"

	"golang.org/x/net/context"

	pb "github.com/BrobridgeOrg/gravity-api/service/exporter"
	app "github.com/BrobridgeOrg/gravity-exporter-rest/pkg/app"
	"github.com/spf13/viper"
)

var transport = &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

type Service struct {
	app app.App
}

func NewService(a app.App) *Service {

	service := &Service{
		app: a,
	}

	return service
}

func (service *Service) SendEvent(ctx context.Context, in *pb.SendEventRequest) (*pb.SendEventReply, error) {

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
