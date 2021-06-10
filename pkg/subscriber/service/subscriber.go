package subscriber

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BrobridgeOrg/gravity-exporter-rest/pkg/app"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	gravity_subscriber "github.com/BrobridgeOrg/gravity-sdk/subscriber"
	gravity_state_store "github.com/BrobridgeOrg/gravity-sdk/subscriber/state_store"
	gravity_sdk_types_projection "github.com/BrobridgeOrg/gravity-sdk/types/projection"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var counter uint64 = 0

var transport = &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

var projectionPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_projection.Projection{}
	},
}

type Subscriber struct {
	app        app.App
	stateStore *gravity_state_store.StateStore
	subscriber *gravity_subscriber.Subscriber
	ruleConfig *RuleConfig
}

func NewSubscriber(a app.App) *Subscriber {
	return &Subscriber{
		app: a,
	}
}

func (subscriber *Subscriber) processData(msg *gravity_subscriber.Message) error {
	/*
		id := atomic.AddUint64((*uint64)(&counter), 1)

		if id%100 == 0 {
			log.Info(id)
		}
	*/

	pj := projectionPool.Get().(*gravity_sdk_types_projection.Projection)
	defer projectionPool.Put(pj)

	// Parsing data
	err := gravity_sdk_types_projection.Unmarshal(msg.Event.Data, pj)
	if err != nil {
		return err
	}

	// Getting channels for specific collection
	restRules, ok := subscriber.ruleConfig.Subscriptions[pj.Collection]
	if !ok {
		return errors.New("Getting channels for specific collection error.")
	}

	// Convert projection to record
	payload, err := pj.ToJSON()
	if err != nil {
		return err
	}

	// Send event to each channel
	for _, restRule := range restRules {

		// Getting parameters
		method := strings.ToUpper(restRule.Method)
		uri := restRule.Uri

		for {
			// Create a request
			request, err := http.NewRequest(method, uri, bytes.NewReader(payload))
			if err != nil {
				log.Error(err)
				<-time.After(time.Second * 5)
				continue
			}

			// Preparing header
			request.Header.Add("Gravity-Version", "1.0")
			request.Header.Add("Content-Type", "application/json")
			for key, value := range restRule.Headers {
				request.Header.Add(key, value)
			}

			client := http.Client{
				Transport: transport,
			}
			resp, err := client.Do(request)
			if err != nil {
				log.Error(err)
				<-time.After(time.Second * 5)
				continue
			}

			// Require body
			defer resp.Body.Close()

			_, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Error(err)
				<-time.After(time.Second * 5)
				continue
			}

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Error("Target HTTP server return with error status code")
				<-time.After(time.Second * 5)
				continue
			}
			break
		}

	}

	msg.Ack()

	return nil
}

func (subscriber *Subscriber) LoadConfigFile(filename string) (*RuleConfig, error) {

	// Open and read config file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	// Parse config
	var config RuleConfig
	json.Unmarshal(byteValue, &config)

	return &config, nil
}

func (subscriber *Subscriber) Init() error {

	// Load rules
	ruleFile := viper.GetString("rules.subscription")

	log.WithFields(log.Fields{
		"ruleFile": ruleFile,
	}).Info("Loading rules...")

	ruleConfig, err := subscriber.LoadConfigFile(ruleFile)
	if err != nil {
		return err
	}

	subscriber.ruleConfig = ruleConfig

	// Load state
	err = subscriber.InitStateStore()
	if err != nil {
		return err
	}

	host := viper.GetString("gravity.host")

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Initializing gravity subscriber")

	// Initializing gravity subscriber and connecting to server
	viper.SetDefault("subscriber.worker_count", 4)
	options := gravity_subscriber.NewOptions()
	options.Verbose = viper.GetBool("subscriber.verbose")
	options.StateStore = subscriber.stateStore
	options.WorkerCount = viper.GetInt("subscriber.worker_count")

	subscriber.subscriber = gravity_subscriber.NewSubscriber(options)
	opts := core.NewOptions()
	err = subscriber.subscriber.Connect(host, opts)
	if err != nil {
		return err
	}

	// Setup data handler
	subscriber.subscriber.SetEventHandler(subscriber.eventHandler)

	// Register subscriber
	log.Info("Registering subscriber")
	subscriberID := viper.GetString("subscriber.subscriber_id")
	subscriberName := viper.GetString("subscriber.subscriber_name")
	err = subscriber.subscriber.Register(gravity_subscriber.SubscriberType_Exporter, "rest", subscriberID, subscriberName)
	if err != nil {
		return err
	}

	// Subscribe to collections
	stc := make(map[string][]string, len(subscriber.ruleConfig.Subscriptions))
	for collection, rules := range subscriber.ruleConfig.Subscriptions {
		var ruleSlice []string
		for _, rule := range rules {
			r, _ := json.Marshal(rule)
			ruleSlice = append(ruleSlice, string(r))
		}

		stc[collection] = ruleSlice
	}

	err = subscriber.subscriber.SubscribeToCollections(stc)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{}).Info("Subscribing to gravity pipelines...")
	err = subscriber.subscriber.AddAllPipelines()
	if err != nil {
		return err
	}

	return nil
}

func (subscriber *Subscriber) eventHandler(msg *gravity_subscriber.Message) {

	err := subscriber.processData(msg)
	if err != nil {
		log.Error(err)
		return
	}
}

func (subscriber *Subscriber) Run() error {

	log.WithFields(log.Fields{}).Info("Starting to fetch data from gravity...")

	subscriber.subscriber.Start()

	return nil
}
