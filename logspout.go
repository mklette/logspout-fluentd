package fluentd

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"
	"time"

	"github.com/gliderlabs/logspout/router"
)

// FluentdAdapter is an adapter for streaming JSON to a fluentd collector.
type FluentdAdapter struct {
	conn      net.Conn
	route     *router.Route
	transport router.AdapterTransport
}

func init() {
	router.AdapterFactories.Register(NewFluentdAdapter, "fluentd")
}

// NewFluentdAdapter creates a Logspout fluentd adapter instance.
func NewFluentdAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("tcp"))
	if !found {
		return nil, errors.New("bad transport: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &FluentdAdapter{
		conn:      conn,
		route:     route,
		transport: transport,
	}, nil
}

// Stream handles a stream of messages from Logspout. Implements router.logAdapter.
func (adapter *FluentdAdapter) Stream(logstream chan *router.Message) {
	for message := range logstream {
		timestamp := int32(time.Now().Unix())
		tag := "docker/" + message.Container.Config.Hostname

		record := make(map[string]string)
		record["message"] = message.Data
		record["host"] = message.Container.Config.Hostname
		record["ident"] = "docker-container-log"
		record["docker/id"] = message.Container.ID
		record["docker/image"] = message.Container.Config.Image
		record["docker/name"] = message.Container.Name
		for key, value := range message.Container.Config.Labels {
			label := strings.Replace(key, ".", "-", -1)
			record["docker/label/"+label] = value
		}
		for key, value := range message.Container.Config.Env {
			// add additional  mesos/chronos information
			label := strings.Replace(key, ".", "-", -1)
			if strings.Contains(label, "MESOS_TASK_ID") ||  strings.Contains(label, "mesos_task_id") {
				record["docker/env/"+label] = value
			}
		}
		data := []interface{}{tag, timestamp, record}

		json, err := json.Marshal(data)
		if err != nil {
			log.Println("fluentd-adapter: ", err)
			continue
		}

		_, err = adapter.conn.Write(json)
		if err != nil {
			err = adapter.retry(json, err)
			if err != nil {
				log.Println("fluentd-adapter: ", err)
				return
			}
		}
	}
}

func (adapter *FluentdAdapter) retry(json []byte, err error) error {
	if opError, ok := err.(*net.OpError); ok {
		if opError.Temporary() || opError.Timeout() {
			retryErr := adapter.retryTemporary(json)
			if retryErr == nil {
				return nil
			}
		}
	}

	return adapter.reconnect()
}

func (adapter *FluentdAdapter) retryTemporary(json []byte) error {
	log.Println("fluentd-adapter: retrying tcp up to 11 times")
	err := retryExp(func() error {
		_, err := adapter.conn.Write(json)
		if err == nil {
			log.Println("fluentd-adapter: retry successful")
			return nil
		}

		return err
	}, 11)

	if err != nil {
		log.Println("fluentd-adapter: retry failed")
		return err
	}

	return nil
}

func (adapter *FluentdAdapter) reconnect() error {
	log.Println("fluentd-adapter: reconnecting forever")

	for {
		conn, err := adapter.transport.Dial(adapter.route.Address, adapter.route.Options)
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}

		log.Println("fluentd-adapter: reconnected")

		adapter.conn = conn
		return nil
	}
}

func retryExp(fun func() error, tries uint) error {
	try := uint(0)
	for {
		err := fun()
		if err == nil {
			return nil
		}

		try++
		if try > tries {
			return err
		}

		time.Sleep((1 << try) * 10 * time.Millisecond)
	}
}
