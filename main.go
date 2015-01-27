package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strings"

	pb "github.com/cheggaaa/pb"
	goflags "github.com/jessevdk/go-flags"
)

type Indexes map[string]interface{}

type Document struct {
	action string
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
	DocChan   chan Document
	ErrChan   chan error
	FlushChan chan struct{}
	QuitChan  chan struct{}
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
	//CompareIndexes     []string `long:"diff" description:"Compare indexes between elasticsearch indexes"`
}

func main() {

	runtime.GOMAXPROCS(2)

	c := Config{
		DocChan:   make(chan Document),
		ErrChan:   make(chan error),
		FlushChan: make(chan struct{}, 1),
		QuitChan:  make(chan struct{}),
	}

	_, err := goflags.Parse(&c)
	if err != nil {
		fmt.Println(err)
		return
	}

	//	if len(c.CompareIndexes) > 0 {
	//		c.Compare()
	//		return
	//	}

	// get all indexes
	idxs := Indexes{}
	if err := c.GetIndexes(&idxs); err != nil {
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

	scroll, err := c.NewScroll()
	if err != nil {
		fmt.Println(err)
		return
	}

	bar := pb.StartNew(scroll.Hits.Total)
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
				bar.Increment()
			case <-c.FlushChan:
				buf.WriteRune('\n')
				c.BulkPost(&buf)
				buf.Reset()
			case <-c.QuitChan:
				fmt.Println("Indexed ", docCount, "documents")
				os.Exit(0) // screw cleaning up (troll)
			}
		}
	}()

	go func() {
		for {
			err := <-c.ErrChan
			fmt.Println(err)
		}
	}()

	for {
		scroll.Stream(&c)
	}
}

func (c *Config) GetIndexes(idx *Indexes) (err error) {

	resp, err := http.Get(fmt.Sprintf("%s/%s/_mappings", c.SrcEs, c.IndexNames))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(idx)

	// remove indexes that start with . if user asked for it
	if c.CopyDotnameIndexes == false {
		for name, _ := range *idx {
			if name[0] == '.' {
				delete(*idx, name)
			}
		}
	}

	// if _all indexes, limit the list of indexes to only these that we kept after looking at mappings
	if c.IndexNames == "_all" {
		var newIndexes []string
		for name, _ := range *idx {
			newIndexes = append(newIndexes, name)
		}
		c.IndexNames = strings.Join(newIndexes, ",")
	}

	return
}

// CreateIndexes on remote ES instance
func (c *Config) CreateIndexes(idxs *Indexes) (err error) {

	for name, idx := range *idxs {
		fmt.Println("create index: ", name)
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

func (c *Config) DeleteIndexes(idxs *Indexes) (err error) {

	for name, idx := range *idxs {
		fmt.Println("deleting index: ", name)
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
		return fmt.Errorf("failed deleting index: %s", string(b))
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
			index.(map[string]interface{})["settings"].(map[string]interface{})["index"] = map[string]interface{}{
				"number_of_replicas": settings.(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"],
				"number_of_shards":   settings.(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_shards"],
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

// stream from source es instance
func (s *Scroll) Stream(c *Config) {

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
		c.QuitChan <- struct{}{}
		return
	}

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
			d := Document{
				Index:  doc["_index"].(string),
				Type:   doc["_type"].(string),
				source: doc["_source"].(map[string]interface{}),
				Id:     doc["_id"].(string),
			}
			c.DocChan <- d
		}

		c.FlushChan <- struct{}{}
	}

	return
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

func (c *Config) Compare() {

}
