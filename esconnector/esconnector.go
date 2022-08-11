package esconnector

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/briwagner/egrid/models"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type ESConnector struct {
	Config elasticsearch.Config
	Client *elasticsearch.Client
}

// NewESConnector creates a new connector.
func NewESConnector(cfg elasticsearch.Config) ESConnector {
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	esc := ESConnector{
		Config: cfg,
		Client: client,
	}
	return esc
}

// ESCheck tries to connect to ES at the specified config.
func (esc *ESConnector) ESCheck() (string, error) {
	cl := *esc.Client
	resp, err := cl.Info()
	if err != nil {
		return "", err
	}
	return resp.String(), nil
}

// AddIndex creates a new on ES, if not exists.
func (esc *ESConnector) AddIndex(name string) error {
	resp, err := esc.Client.Indices.Exists([]string{name})
	if err != nil {
		return err
	}
	if resp.StatusCode == 404 {
		// Create index.
		log.Printf("index doesn't exist: %s\n", name)
		resp, err = esc.Client.Indices.Create(name)
		if err != nil || resp.IsError() {
			return err
		}
		log.Printf("New index created: %s\n", name)
	}
	return nil
}

// PutRecord adds a record to Elastic index.
func (esc *ESConnector) PutRecord(index string, records []models.PlantYear) {
	var wg sync.WaitGroup

	for _, rec := range records {
		data, err := json.Marshal(rec)
		if err != nil {
			log.Panic(err)
			continue
		}

		wg.Add(1)

		go func(id string, dd []byte) {
			defer wg.Done()

			req := esapi.IndexRequest{
				Index:      index,
				DocumentID: id,
				Body:       bytes.NewReader(data),
			}

			// Perform the request with the client.
			res, err := req.Do(context.Background(), esc.Client)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()
		}(rec.GetID(), data)

		wg.Wait()
	}
}
