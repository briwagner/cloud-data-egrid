package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/briwagner/egrid/esconnector"
	"github.com/briwagner/egrid/models"
	"github.com/elastic/go-elasticsearch/v7"
)

func main() {
	var fileUrl = flag.String("fileUrl", "", "URL for CSV data file.")
	var elasticUrl = flag.String("elasticUrl", "http://localhost:9200", "URL for Elastic service")
	flag.Parse()

	if *fileUrl == "" {
		log.Fatal("fileUrl not set. Must pass URL for file, e.g. 'http://localhost/file.csv'")
	}

	res, err := GetRecords(*fileUrl)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Record count: %d\n", len(res.Data))

	cfg := elasticsearch.Config{
		Addresses: []string{
			*elasticUrl,
		},
	}
	esc := esconnector.NewESConnector(cfg)
	log.Println(esc.ESCheck())

	index := "plantyear"
	esc.AddIndex(index)
	log.Printf("Adding records to index: %s\n", index)
	esc.PutRecord(index, res.Data)
	log.Println("Completed adding records")
}

// GetRecords retrieves a file and returns records as PlantYear struct type.
func GetRecords(fileUrl string) (models.Results, error) {
	var res models.Results

	resp, err := http.Get(fileUrl)
	if err != nil {
		return res, err
	}
	if resp.StatusCode != http.StatusOK {
		return res, fmt.Errorf("file not found: %s", fileUrl)
	}
	defer resp.Body.Close()

	scanner := models.NewPlantScanner(resp.Body)
	for scanner.Scan() {
		if err := scanner.Error(); err != nil {
			log.Panic(err)
			continue
		}

		res.Data = append(res.Data, scanner.Record())
	}

	return res, nil
}
