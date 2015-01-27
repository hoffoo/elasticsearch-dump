package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"strconv"

	goflags "github.com/jessevdk/go-flags"
)

type Indexes map[string]interface{}

type Document struct {
	action string
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	Id     int                    `json:"_id"`
	source map[string]interface{} `json:"_source"`
}

type Scroll struct {
	ScrollId string `json:"_scroll_id"`
	TimedOut bool   `json:"timed_out"`
}

type Config struct {
	DocChan   chan Document
	ErrChan   chan error
	FlushChan chan struct{}
	Uid       string // es scroll uid

	// config options
	SrcEs          string `short:"s" long:"source" description:"Source elasticsearch instance" required:"true"`
	DstEs          string `short:"d" long:"dest" description:"Destination elasticsearch instance" required:"true"`
	DocBufferCount int    `short:"c" long:"count" description:"Number of documents at a time: ie \"size\" in the scroll request" default:"100"`
	ScanTime       string `short:"t" long:"time" description:"Scroll time" default:"1m"`
}

func main() {

	runtime.GOMAXPROCS(2)

	c := Config{
		DocChan:   make(chan Document),
		ErrChan:   make(chan error),
		FlushChan: make(chan struct{}, 1),
	}

	goflags.Parse(&c)

	// get all indexes
	idxs := Indexes{}
	if err := c.GetIndexes(&idxs); err != nil {
		fmt.Println(err)
		return
	}

	// create indexes on DstEs
	if err := c.CreateIndexes(&idxs); err != nil {
		fmt.Println(err)
		return
	}

	scroll, err := c.NewScroll()
	if err != nil {
		fmt.Println(err)
		return
	}

	go func() {
		buf := bytes.Buffer{}
		enc := json.NewEncoder(&buf)
		var docCount int
		for {
			select {
			case doc := <-c.DocChan:
				post := map[string]Document{
					"create": doc,
				}
				if err = enc.Encode(post); err != nil {
					c.ErrChan <- err
				}
				if err = enc.Encode(doc.source); err != nil {
					c.ErrChan <- err
				}
				docCount++
			case <-c.FlushChan:
				fmt.Println(docCount)
				buf.WriteRune('\n')
				c.BulkPost(&buf)
				buf.Reset()
			case err := <-c.ErrChan:
				fmt.Println(err)
			}
		}
	}()

	for {
		scroll.Stream(&c)
	}
}

func (c *Config) GetIndexes(idx *Indexes) (err error) {

	resp, err := http.Get(fmt.Sprintf("%s/_mappings", c.SrcEs))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(idx)

	return
}

//
func (c *Config) CreateIndexes(idxs *Indexes) (err error) {

	for name, idx := range *idxs {
		fmt.Println(name)
		body := bytes.Buffer{}
		enc := json.NewEncoder(&body)
		enc.Encode(idx)

		resp, err := http.Post(fmt.Sprintf("%s/%s", c.DstEs, name), "", &body)
		if err != nil {
			return err
		}

		if resp.StatusCode != 200 {
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("failed creating index: %s", string(b))
		}

		resp.Body.Close()
	}

	return
}

// make the initial scroll req
func (c *Config) NewScroll() (scroll *Scroll, err error) {

	resp, err := http.Get(fmt.Sprintf("%s/_all/_search/?scroll=1m&search_type=scan&size=%d", c.SrcEs, c.DocBufferCount))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)

	scroll = &Scroll{}
	err = dec.Decode(scroll)

	return
}

// stream from source es instance
func (s *Scroll) Stream(c *Config) {

	id := bytes.NewBufferString(s.ScrollId)
	resp, err := http.Post(fmt.Sprintf("%s/_search/scroll?scroll=%s&search_type=scan&size=%d", c.SrcEs, c.ScanTime, c.DocBufferCount), "", id)
	if err != nil {
		c.ErrChan <- err
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		c.ErrChan <- fmt.Errorf("bad scroll response: %s", string(b))
		return
	}

	dec := json.NewDecoder(resp.Body)
	docs := map[string]interface{}{}
	err = dec.Decode(&docs)
	if err != nil {
		c.ErrChan <- err
	} else {
		hits := docs["hits"]
		var ok bool
		var docsInterface map[string]interface{}
		if docsInterface, ok = hits.(map[string]interface{}); !ok {
			c.ErrChan <- errors.New("failed casting doc interfaces")
			return
		}

		var docs []interface{}
		if docs, ok = docsInterface["hits"].([]interface{}); !ok {
			c.ErrChan <- errors.New("failed casting doc")
			return
		}

		for _, docI := range docs {
			doc := docI.(map[string]interface{})
			id, _ := strconv.Atoi(doc["_id"].(string))
			d := Document{
				Index:  doc["_index"].(string),
				Type:   doc["_type"].(string),
				source: doc["_source"].(map[string]interface{}),
				Id:     id,
			}
			delete(d.source, "id") // dont put id thats in _source
			c.DocChan <- d
		}

		c.FlushChan <- struct{}{}
	}
}

func (c *Config) BulkPost(data *bytes.Buffer) {

	resp, err := http.Post(fmt.Sprintf("%s/_bulk", c.DstEs), "", data)
	if err != nil {
		c.ErrChan <- err
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		c.ErrChan <- fmt.Errorf("bad bulk response: %s", string(b))
		return
	}
}
