package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"gopkg.in/olivere/elastic.v5"
)

const (
	elasticIndexName = "julia_test"
	elasticTypeName  = "document"
)

//Meta struct object
type Meta struct {
	ArchivedTime string `json:"archivedTime"`
	BusinessTime string `json:"businessTime"`
	CountryCode  string `json:"countryCode"`
}

//Document struct object
type Document struct {
	ID       string `json:"id"`
	Client   string `json:"client"`
	Meta     Meta   `json:"meta"`
	Path     string `json:"path"`
	PathType string `json:"pathType"`
	Type     string `json:"type"`
}

var (
	elasticClient *elastic.Client
)

func home(w http.ResponseWriter, r *http.Request) {
	var err error

	for {
		elasticClient, err = elastic.NewClient(
			elastic.SetURL("http://essa0.essa.test9.mcc.be-gcw1.metroscales.io:9200/"),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Println(err)
			time.Sleep(3 * time.Second)
		} else {
			break
		}
	}
	c := context.Background()

	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case "GET":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "get called"}`))

		// query := elastic.NewRegexpQuery("id", ".*chn.*|.*gtp.*")
		query := elastic.NewRegexpQuery("id", ".*42314.*")
		searchResult, err := elasticClient.Search().
			Index(elasticIndexName). // search in index "tmpindex"
			Query(query).
			Size(4500).
			Do(c) // execute
		if err != nil {
			log.Fatalln(err)
			return
		}
		log.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
		if searchResult.Hits.TotalHits > 0 {
			log.Printf("Found a total of %d documents\n", searchResult.Hits.TotalHits)
			w.Write([]byte(`{"found documents"}`))
			// Iterate through results
			for _, hit := range searchResult.Hits.Hits {
				// hit.Index contains the name of the index
				// Deserialize hit.Source into a Transaction (could also be just a map[string]interface{}).
				var t Document
				err := json.Unmarshal(*hit.Source, &t)
				if err != nil {
					// Deserialization failed
					log.Panic(err)
				}
				// Work with Transaction
				log.Printf("Document by %s: mnt: %v archivedTime:%v businessTime:%v\n", hit.Id, t.Path, t.Meta.ArchivedTime, t.Meta.BusinessTime)
			}
		} else {
			// No hits
			log.Print("Found no documents\n")
			w.Write([]byte(`{"found no documents"}`))
		}

	default:
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message": "not found"}`))
	}
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", home)
	log.Fatal(http.ListenAndServe(":8080", r))
}
