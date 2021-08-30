package subscriber

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BrobridgeOrg/gravity-exporter-rest/pkg/app"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	gravity_subscriber "github.com/BrobridgeOrg/gravity-sdk/subscriber"
	gravity_state_store "github.com/BrobridgeOrg/gravity-sdk/subscriber/state_store"

	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var counter uint64 = 0

var transport = &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

type Packet struct {
	EventName  string                 `json:"event"`
	Collection string                 `json:"collection"`
	Payload    map[string]interface{} `json:"payload"`
	Meta       map[string]interface{} `json:"meta"`
}

var packetPool = sync.Pool{
	New: func() interface{} {
		return &Packet{}
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

	event := msg.Payload.(*gravity_subscriber.DataEvent)
	pj := event.Payload

	// Getting channels for specific collection
	restRules, ok := subscriber.ruleConfig.Subscriptions[pj.Collection]
	if !ok {
		// skip
		return nil
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

	// Initializing gravity node information
	viper.SetDefault("gravity.domain", "gravity")
	domain := viper.GetString("gravity.domain")
	host := viper.GetString("gravity.host")

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Initializing gravity subscriber")

	// Initializing gravity subscriber and connecting to server
	viper.SetDefault("subscriber.workerCount", 4)
	options := gravity_subscriber.NewOptions()
	options.Verbose = viper.GetBool("subscriber.verbose")
	options.Domain = domain
	options.StateStore = subscriber.stateStore
	options.WorkerCount = viper.GetInt("subscriber.workerCount")
	options.ChunkSize = viper.GetInt("subscriber.chunkSize")
	options.InitialLoad.Enabled = viper.GetBool("initialLoad.enabled")
	options.InitialLoad.OmittedCount = viper.GetUint64("initialLoad.omittedCount")

	// Loading access key
	viper.SetDefault("subscriber.appID", "anonymous")
	viper.SetDefault("subscriber.accessKey", "")
	options.Key = keyring.NewKey(viper.GetString("subscriber.appID"), viper.GetString("subscriber.accessKey"))

	subscriber.subscriber = gravity_subscriber.NewSubscriber(options)
	opts := core.NewOptions()
	err = subscriber.subscriber.Connect(host, opts)
	if err != nil {
		return err
	}

	// Setup data handler
	subscriber.subscriber.SetEventHandler(subscriber.eventHandler)
	subscriber.subscriber.SetSnapshotHandler(subscriber.snapshotHandler)

	// Register subscriber
	log.Info("Registering subscriber")
	subscriberID := viper.GetString("subscriber.subscriberID")
	subscriberName := viper.GetString("subscriber.subscriberName")
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

	// Subscribe to pipelines
	err = subscriber.initializePipelines()
	if err != nil {
		return err
	}

	return nil
}

func (subscriber *Subscriber) initializePipelines() error {

	// Subscribe to pipelines
	log.WithFields(log.Fields{}).Info("Subscribing to gravity pipelines...")
	viper.SetDefault("subscriber.pipelineStart", 0)
	viper.SetDefault("subscriber.pipelineEnd", -1)

	pipelineStart := viper.GetInt64("subscriber.pipelineStart")
	pipelineEnd := viper.GetInt64("subscriber.pipelineEnd")

	// Subscribe to all pipelines
	if pipelineStart == 0 && pipelineEnd == -1 {
		err := subscriber.subscriber.AddAllPipelines()
		if err != nil {
			return err
		}

		return nil
	}

	// Subscribe to pipelines in then range
	if pipelineStart < 0 {
		return fmt.Errorf("subscriber.pipelineStart should be higher than -1")
	}

	if pipelineStart > pipelineEnd {
		if pipelineEnd != -1 {
			return fmt.Errorf("subscriber.pipelineStart should be less than subscriber.pipelineEnd")
		}
	}

	count, err := subscriber.subscriber.GetPipelineCount()
	if err != nil {
		return err
	}

	if pipelineEnd == -1 {
		pipelineEnd = int64(count) - 1
	}

	pipelines := make([]uint64, 0, pipelineEnd-pipelineStart)
	for i := pipelineStart; i <= pipelineEnd; i++ {
		pipelines = append(pipelines, uint64(i))
	}

	err = subscriber.subscriber.SubscribeToPipelines(pipelines)
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

func (subscriber *Subscriber) snapshotHandler(msg *gravity_subscriber.Message) {

	event := msg.Payload.(*gravity_subscriber.SnapshotEvent)
	snapshotRecord := event.Payload

	// Getting tables for specific collection
	restRules, ok := subscriber.ruleConfig.Subscriptions[event.Collection]
	if !ok {
		return
	}

	// Send event to each channel
	for _, restRule := range restRules {

		// Getting parameters
		method := strings.ToUpper(restRule.Method)
		uri := restRule.Uri

		// TODO: using batch mechanism to improve performance
		packet := packetPool.Get().(*Packet)
		for {
			payload := snapshotRecord.Payload.AsMap()

			packet.EventName = "snapshot"
			packet.Payload = payload
			packet.Collection = event.Collection

			if snapshotRecord.Meta != nil {
				packet.Meta = snapshotRecord.Meta.AsMap()
			}

			//convert to byte
			newPacket, err := jsoniter.Marshal(packet)
			if err != nil {
				log.Error(err)
				<-time.After(time.Second * 5)
				continue
			}

			// Create a request
			request, err := http.NewRequest(method, uri, bytes.NewReader(newPacket))
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

			packetPool.Put(packet)
			break
		}
	}
	msg.Ack()
}

func (subscriber *Subscriber) Run() error {

	log.WithFields(log.Fields{}).Info("Starting to fetch data from gravity...")

	subscriber.subscriber.Start()

	return nil
}
