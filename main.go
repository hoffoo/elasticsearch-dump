package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"sync"

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

type Config struct {
	FlushLock sync.Mutex
	DocChan   chan Document
	ErrChan   chan error
	Uid       string // es scroll uid

	// config options
	SrcEs              string `short:"s" long:"source" description:"Source elasticsearch instance" required:"true"`
	DstEs              string `short:"d" long:"dest" description:"Destination elasticsearch instance" required:"true"`
	DocBufferCount     int    `short:"c" long:"count" description:"Number of documents at a time: ie \"size\" in the scroll request" default:"100"`
	ScrollTime         string `short:"t" long:"time" description:"Scroll time" default:"1m"`
	CopySettings       bool   `long:"settings" description:"Copy sharding and replication settings from source" default:"true"`
	Destructive        bool   `short:"f" long:"force" description:"Delete destination index before copying" default:"false"`
	IndexNames         string `short:"i" long:"indexes" description:"List of indexes to copy, comma separated" default:"_all"`
	CopyDotnameIndexes bool   `short:"a" long:"all" description:"Copy indexes starting with ." default:"false"`
	Workers            int    `short:"w" long:"workers" description:"Concurrency" default:"1"`
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
	c.DocChan = make(chan Document, c.DocBufferCount*c.Workers)

	// get all indexes from source
	idxs := Indexes{}
	if err := c.GetIndexes(c.SrcEs, &idxs); err != nil {
		fmt.Println(err)
		return
	}

	// copy index settings if user asked
	if c.CopySettings == true {
		if err := c.CopyReplicationAndShardingSettings(&idxs); err != nil {
			fmt.Println(err)
			return
		}
	}

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
	for scroll.Stream(&c) == false {
	}

	// finished, flush any remaining docs and quit
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
		doc, open := <-c.DocChan

		// if channel is closed flush and gtfo
		if !open {
			goto WORKER_DONE
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

		// if we hit 100mb limit, flush to es and reset mainBuf
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

func (c *Config) GetIndexes(host string, idx *Indexes) (err error) {

	resp, err := http.Get(fmt.Sprintf("%s/%s/_mapping", host, c.IndexNames))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(idx)

	// always ignore internal _ indexes
	for name, _ := range *idx {
		if name[0] == '_' {
			delete(*idx, name)
		}
	}

	// remove indexes that start with . if user asked for it
	if c.CopyDotnameIndexes == false {
		for name, _ := range *idx {
			if name[0] == '.' {
				delete(*idx, name)
			}
		}
	}

	// if _all indexes limit the list of indexes to only these that we kept
	// after looking at mappings
	if c.IndexNames == "_all" {
		var newIndexes []string
		for name, _ := range *idx {
			newIndexes = append(newIndexes, name)
		}
		c.IndexNames = strings.Join(newIndexes, ",")
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

func (c *Config) CopyReplicationAndShardingSettings(idxs *Indexes) (err error) {

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
			var shards, replicas string
			if _, ok := index.(map[string]interface{})["settings"].(map[string]interface{})["index"]; ok {
				// try the new style syntax first, which has an index object
				shards = settings.(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_shards"].(string)
				replicas = settings.(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"].(string)
			} else {
				// if not, could be running from an old es intace, try the old style index.number_of_shards
				shards = settings.(map[string]interface{})["settings"].(map[string]interface{})["index.number_of_shards"].(string)
				replicas = settings.(map[string]interface{})["settings"].(map[string]interface{})["index.number_of_replicas"].(string)
			}
			index.(map[string]interface{})["settings"].(map[string]interface{})["index"] = map[string]interface{}{
				"number_of_shards":   shards,
				"number_of_replicas": replicas,
			}
		}
	}

	return
}

// make the initial scroll req
func (c *Config) NewScroll() (scroll *Scroll, err error) {

	resp, err := http.Get(fmt.Sprintf("%s/%s/_search/?scroll=1m&search_type=scan&size=%d", c.SrcEs, c.IndexNames, c.DocBufferCount))
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
func (s *Scroll) Stream(c *Config) (done bool) {

	id := bytes.NewBufferString(s.ScrollId)
	resp, err := http.Post(fmt.Sprintf("%s/_search/scroll?scroll=%s&search_type=scan&size=%d", c.SrcEs, c.ScrollTime, c.DocBufferCount), "", id)
	if err != nil {
		c.ErrChan <- err
		return
	}
	defer resp.Body.Close()

	// XXX this might be bad, but assume we are done
	if resp.StatusCode == 404 {
		// flush and quit
		return true
	}

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		c.ErrChan <- fmt.Errorf("bad scroll response: %s", string(b))
		return
	}

	c.DecodeStream(resp.Body)

	return
}

func (c *Config) DecodeStream(body io.Reader) {

	dec := json.NewDecoder(body)
	streamResponse := map[string]interface{}{}
	err := dec.Decode(&streamResponse)
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
		doc := docI.(map[string]interface{})
		c.DocChan <- Document{
			Index:  doc["_index"].(string),
			Type:   doc["_type"].(string),
			source: doc["_source"].(map[string]interface{}),
			Id:     doc["_id"].(string),
		}
	}
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
