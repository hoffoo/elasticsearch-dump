package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	pb "github.com/cheggaaa/pb"
	goflags "github.com/jessevdk/go-flags"
)

type Indexes map[string]interface{}

type Document struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	Id     string                 `json:"_id"`
	source map[string]interface{} `json:"_source"`
}

type Scroll struct {
	ScrollId string `json:"_scroll_id"`
	TimedOut bool   `json:"timed_out"`
	Hits     struct {
		Total int `json:"total"`
	} `json:"hits"`
}

type ClusterHealth struct {
	Name   string `json:"cluster_name"`
	Status string `json:"status"`
}

type Config struct {
	FlushLock sync.Mutex
	DocChan   chan map[string]interface{}
	ErrChan   chan error
	Uid       string // es scroll uid

	// config options
	SrcEs             string `short:"s" long:"source"  description:"source elasticsearch instance" required:"true"`
	DstEs             string `short:"d" long:"dest"    description:"destination elasticsearch instance" required:"true"`
	DocBufferCount    int    `short:"c" long:"count"   description:"number of documents at a time: ie \"size\" in the scroll request" default:"100"`
	ScrollTime        string `short:"t" long:"time"    description:"scroll time" default:"1m"`
	Destructive       bool   `short:"f" long:"force"   description:"delete destination index before copying" default:"false"`
	ShardsCount       int    `long:"shards"            description:"set a number of shards on newly created indexes"`
	DocsOnly          bool   `long:"docs-only"         description:"load documents only, do not try to recreate indexes" default:"false"`
	CreateIndexesOnly bool   `long:"index-only"        description:"only create indexes, do not load documents" default:"false"`
	EnableReplication bool   `long:"replicate"         description:"enable replication while indexing into the new indexes" default:"false"`
	IndexNames        string `short:"i" long:"indexes" description:"list of indexes to copy, comma separated" default:"_all"`
	CopyAllIndexes    bool   `short:"a" long:"all"     description:"copy indexes starting with . and _" default:"false"`
	Workers           int    `short:"w" long:"workers" description:"concurrency" default:"1"`
	CopySettings      bool   `long:"settings"          description:"copy sharding settings from source" default:"true"`
	WaitForGreen      bool   `long:"green"             description:"wait for both hosts cluster status to be green before dump. otherwise yellow is okay" default:"false"`
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	c := Config{
		FlushLock: sync.Mutex{},
		ErrChan:   make(chan error),
	}

	// parse args
	_, err := goflags.Parse(&c)
	if err != nil {
		fmt.Println(err)
		return
	}

	// enough of a buffer to hold all the search results across all workers
	c.DocChan = make(chan map[string]interface{}, c.DocBufferCount*c.Workers)

	// get all indexes from source
	idxs := Indexes{}
	if err := c.GetIndexes(c.SrcEs, &idxs); err != nil {
		fmt.Println(err)
		return
	}

	// copy index settings if user asked
	if c.ShardsCount > 0 {
		for name, _ := range idxs {
			idxs.SetShardCount(name, fmt.Sprint(c.ShardsCount))
		}
	} else if c.CopySettings == true {
		if err := c.CopyShardingSettings(&idxs); err != nil {
			fmt.Println(err)
			return
		}
	}

	// disable replication
	if c.EnableReplication == false {
		idxs.DisableReplication()
	}

	if c.DocsOnly == false {
		// delete remote indexes if user asked
		if c.Destructive == true {
			if err := c.DeleteIndexes(&idxs); err != nil {
				fmt.Println(err)
				return
			}
		}

		// create indexes on DstEs
		if err := c.CreateIndexes(&idxs); err != nil {
			fmt.Println(err)
			return
		}
	}

	// if we only want to create indexes, we are done here, return
	if c.CreateIndexesOnly {
		fmt.Println("Indexes created, done")
		return
	}

	// wait for cluster state to be okay before dumping
	timer := time.NewTimer(time.Second * 3)
	for {
		if status, ready := c.ClusterReady(c.SrcEs); !ready {
			fmt.Printf("%s at %s is %s, delaying dump\n", status.Name, c.SrcEs, status.Status)
			<-timer.C
			continue
		}
		if status, ready := c.ClusterReady(c.DstEs); !ready {
			fmt.Printf("%s at %s is %s, delaying dump\n", status.Name, c.DstEs, status.Status)
			<-timer.C
			continue
		}

		timer.Stop()
		break
	}
	fmt.Println("starting dump..")

	// start scroll
	scroll, err := c.NewScroll()
	if err != nil {
		fmt.Println(err)
		return
	}

	// create a progressbar and start a docCount
	bar := pb.StartNew(scroll.Hits.Total)
	var docCount int

	wg := sync.WaitGroup{}
	wg.Add(c.Workers)
	for i := 0; i < c.Workers; i++ {
		go c.NewWorker(&docCount, bar, &wg)
	}

	// print errors
	go func() {
		for {
			err := <-c.ErrChan
			fmt.Println(err)
		}
	}()

	// loop scrolling until done
	for scroll.Next(&c) == false {
	}

	// finished, close doc chan and wait for goroutines to be done
	close(c.DocChan)
	wg.Wait()
	bar.FinishPrint(fmt.Sprintln("Indexed", docCount, "documents"))
}

func (c *Config) NewWorker(docCount *int, bar *pb.ProgressBar, wg *sync.WaitGroup) {

	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	for {
		var err error
		docI, open := <-c.DocChan
		doc := Document{
			Index:  docI["_index"].(string),
			Type:   docI["_type"].(string),
			source: docI["_source"].(map[string]interface{}),
			Id:     docI["_id"].(string),
		}

		// if channel is closed flush and gtfo
		if !open {
			goto WORKER_DONE
		}

		// sanity check
		if len(doc.Index) == 0 || len(doc.Id) == 0 || len(doc.Type) == 0 {
			c.ErrChan <- fmt.Errorf("failed decoding document: %+v", doc)
			continue
		}

		// encode the doc and and the _source field for a bulk request
		post := map[string]Document{
			"create": doc,
		}
		if err = docEnc.Encode(post); err != nil {
			c.ErrChan <- err
		}
		if err = docEnc.Encode(doc.source); err != nil {
			c.ErrChan <- err
		}

		// if we approach the 100mb es limit, flush to es and reset mainBuf
		if mainBuf.Len()+docBuf.Len() > 100000000 {
			c.BulkPost(&mainBuf)
		}

		// append the doc to the main buffer
		mainBuf.Write(docBuf.Bytes())
		// reset for next document
		docBuf.Reset()
		bar.Increment()
		(*docCount)++
	}

WORKER_DONE:
	if docBuf.Len() > 0 {
		mainBuf.Write(docBuf.Bytes())
	}
	c.BulkPost(&mainBuf)
	wg.Done()
}

func (c *Config) GetIndexes(host string, idxs *Indexes) (err error) {

	resp, err := http.Get(fmt.Sprintf("%s/%s/_mapping", host, c.IndexNames))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(idxs)

	// always ignore internal _ indexes
	for name, _ := range *idxs {
		if name[0] == '_' {
			delete(*idxs, name)
		}
	}

	// remove indexes that start with . if user asked for it
	if c.CopyAllIndexes == false {
		for name, _ := range *idxs {
			switch name[0] {
			case '.':
				delete(*idxs, name)
			case '_':
				delete(*idxs, name)

			}
		}
	}

	// if _all indexes limit the list of indexes to only these that we kept
	// after looking at mappings
	if c.IndexNames == "_all" {
		var newIndexes []string
		for name, _ := range *idxs {
			newIndexes = append(newIndexes, name)
		}
		c.IndexNames = strings.Join(newIndexes, ",")
	}

	// wrap in mappings if dumping from super old es
	for name, idx := range *idxs {
		if _, ok := idx.(map[string]interface{})["mappings"]; !ok {
			(*idxs)[name] = map[string]interface{}{
				"mappings": idx,
			}
		}
	}

	return
}

// CreateIndexes on remodeleted ES instance
func (c *Config) CreateIndexes(idxs *Indexes) (err error) {

	for name, idx := range *idxs {
		body := bytes.Buffer{}
		enc := json.NewEncoder(&body)
		enc.Encode(idx)

		resp, err := http.Post(fmt.Sprintf("%s/%s", c.DstEs, name), "", &body)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			b, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf("failed creating index: %s", string(b))
		}

		fmt.Println("created index: ", name)
	}

	return
}

func (c *Config) DeleteIndexes(idxs *Indexes) (err error) {

	for name, idx := range *idxs {
		body := bytes.Buffer{}
		enc := json.NewEncoder(&body)
		enc.Encode(idx)

		req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s", c.DstEs, name), nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		defer resp.Body.Close()
		if resp.StatusCode == 404 {
			// thats okay, index doesnt exist
			continue
		}

		if resp.StatusCode != 200 {
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("failed deleting index: %s", string(b))
		}

		fmt.Println("deleted index: ", name)
	}

	return
}

func (c *Config) CopyShardingSettings(idxs *Indexes) (err error) {

	// get all settings
	allSettings := map[string]interface{}{}

	resp, err := http.Get(fmt.Sprintf("%s/_all/_settings", c.SrcEs))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return fmt.Errorf("failed getting settings for index: %s", string(b))
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&allSettings); err != nil {
		return err
	}

	for name, index := range *idxs {
		if settings, ok := allSettings[name]; !ok {
			return fmt.Errorf("couldnt find index %s", name)
		} else {
			// omg XXX
			index.(map[string]interface{})["settings"] = map[string]interface{}{}
			var shards string
			if _, ok := settings.(map[string]interface{})["settings"].(map[string]interface{})["index"]; ok {
				// try the new style syntax first, which has an index object
				shards = settings.(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_shards"].(string)
			} else {
				// if not, could be running from old es, try the old style index.number_of_shards
				shards = settings.(map[string]interface{})["settings"].(map[string]interface{})["index.number_of_shards"].(string)
			}
			index.(map[string]interface{})["settings"].(map[string]interface{})["index"] = map[string]interface{}{
				"number_of_shards": shards,
			}
		}
	}

	return
}

func (idxs *Indexes) SetShardCount(indexName, shards string) {

	index := (*idxs)[indexName]
	if _, ok := (*idxs)[indexName].(map[string]interface{})["settings"]; !ok {
		index.(map[string]interface{})["settings"] = map[string]interface{}{}
	}

	if _, ok := (*idxs)[indexName].(map[string]interface{})["settings"].(map[string]interface{})["index"]; !ok {
		index.(map[string]interface{})["settings"].(map[string]interface{})["index"] = map[string]interface{}{}
	}

	index.(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_shards"] = shards
}

func (idxs *Indexes) DisableReplication() {

	for name, index := range *idxs {
		if _, ok := (*idxs)[name].(map[string]interface{})["settings"]; !ok {
			index.(map[string]interface{})["settings"] = map[string]interface{}{}
		}

		if _, ok := (*idxs)[name].(map[string]interface{})["settings"].(map[string]interface{})["index"]; !ok {
			index.(map[string]interface{})["settings"].(map[string]interface{})["index"] = map[string]interface{}{}
		}

		index.(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"] = "0"
	}
}

// make the initial scroll req
func (c *Config) NewScroll() (scroll *Scroll, err error) {

	// curl -XGET 'http://es-0.9:9200/_search?search_type=scan&scroll=10m&size=50'
	url := fmt.Sprintf("%s/%s/_search?search_type=scan&scroll=%s&size=%d", c.SrcEs, c.IndexNames, c.ScrollTime, c.DocBufferCount)
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)

	scroll = &Scroll{}
	err = dec.Decode(scroll)

	return
}

// Stream from source es instance. "done" is an indicator that the stream is
// over
func (s *Scroll) Next(c *Config) (done bool) {

	//  curl -XGET 'http://es-0.9:9200/_search/scroll?scroll=5m'
	id := bytes.NewBufferString(s.ScrollId)

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/_search/scroll?scroll=%s", c.SrcEs, c.ScrollTime), id)
	if err != nil {
		c.ErrChan <- err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.ErrChan <- err
	}
	defer resp.Body.Close()

	// XXX this might be bad, but assume we are done
	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		c.ErrChan <- fmt.Errorf("scroll response: %s", string(b))
		// flush and quit
		return true
	}

	// decode elasticsearch scroll response
	dec := json.NewDecoder(resp.Body)
	streamResponse := map[string]interface{}{}
	err = dec.Decode(&streamResponse)
	if err != nil {
		c.ErrChan <- err
		return
	}

	// decode docs json
	hits := streamResponse["hits"]
	var ok bool
	var docsInterface map[string]interface{}
	if docsInterface, ok = hits.(map[string]interface{}); !ok {
		c.ErrChan <- errors.New("failed casting doc interfaces")
		return
	}

	// cast the hits field into an array
	var docs []interface{}
	if docs, ok = docsInterface["hits"].([]interface{}); !ok {
		c.ErrChan <- errors.New("failed casting doc")
		return
	}

	// write all the docs into a channel
	for _, docI := range docs {
		c.DocChan <- docI.(map[string]interface{})
	}

	return
}

// Post to es as bulk and reset the data buffer
func (c *Config) BulkPost(data *bytes.Buffer) {

	c.FlushLock.Lock()
	defer c.FlushLock.Unlock()

	data.WriteRune('\n')
	resp, err := http.Post(fmt.Sprintf("%s/_bulk", c.DstEs), "", data)
	if err != nil {
		c.ErrChan <- err
		return
	}

	defer resp.Body.Close()
	defer data.Reset()
	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		c.ErrChan <- fmt.Errorf("bad bulk response: %s", string(b))
		return
	}
}

func (c *Config) ClusterReady(host string) (*ClusterHealth, bool) {

	health := ClusterStatus(host)
	if health.Status == "red" {
		return health, false
	}

	if c.WaitForGreen == false && health.Status == "yellow" {
		return health, true
	}

	if health.Status == "green" {
		return health, true
	}

	return health, false
}

func ClusterStatus(host string) *ClusterHealth {

	resp, err := http.Get(fmt.Sprintf("%s/_cluster/health", host))
	if err != nil {
		return &ClusterHealth{Name: host, Status: "unreachable"}
	}
	defer resp.Body.Close()

	health := &ClusterHealth{}
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&health)

	return health
}
