package tools

import (
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"heckel.io/elastictl/util"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

func RemovePitId(rootURI string, pitId string) error {
	req, err := http.NewRequest("DELETE", rootURI + "/_pit", strings.NewReader(fmt.Sprintf("{\"id\":\"%s\"}", pitId)))
    req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == 200 {
		return nil
	}
	return nil
}

func Export(host string, index string, search string, w io.Writer) (int, error) {
	log.Printf("exporting index %s/%s", host, index)
	rootURI := fmt.Sprintf("http://%s", host)

	// Dump mapping first
	rootIndexURI := fmt.Sprintf("http://%s/%s", host, index)
	req, err := http.NewRequest("GET", rootIndexURI, nil)
    req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	rawMapping, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	mapping := gjson.GetBytes(rawMapping, index).String()
	if _, err := fmt.Fprintln(w, mapping); err != nil {
		return 0, err
	}

	// create pit
	req, err = http.NewRequest("POST", rootIndexURI + "/_pit?keep_alive=1m", nil)
    req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return 0, err
	}
	resp, err = client.Do(req)
	if err != nil {
		return 0, err
	}
	rawPitId, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	pitId := gjson.GetBytes(rawPitId, "id").String()

	// Initial search request
	var body io.Reader
	if search == "" {
		search = "{}"
	}
	sortQuery := gjson.Get(search, "sort")
	var sortFieldString string
	var sortFieldQuery string
	if ! sortQuery.Exists() {
		sortFieldString = "_id"
		sortFieldQuery = "&sort=_id"
	}

	pitMap := map[string]interface{}{"pit": pitId, "keep_alive": "1m"}
	bodyRaw, _ := sjson.Set(search, "pit", pitMap)
	body = strings.NewReader(bodyRaw)
	uri := fmt.Sprintf("%s/_search?size=10000%s", rootURI, sortFieldQuery)
	req, err = http.NewRequest("POST", uri, body)
    req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return 0, err
	}
	resp, err = client.Do(req)
	if err != nil {
		RemovePitId(rootURI, pitId)
		return 0, err
	}
	if resp.Body == nil {
		RemovePitId(rootURI, pitId)
		return 0, err
	}

	var progress *util.ProgressBar
	exported := 0

	for {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			RemovePitId(rootURI, pitId)
			return 0, err
		}

		if progress == nil {
			total := gjson.GetBytes(body, "hits.total")
			if !total.Exists() {
				RemovePitId(rootURI, pitId)
				return 0, errors.New("no total")
			}
			progress = util.NewProgressBarWithTotal(os.Stderr, int(total.Int()))
		}

		hits := gjson.GetBytes(body, "hits.hits")
		if !hits.Exists() || !hits.IsArray() {
			RemovePitId(rootURI, pitId)
			return 0, errors.New("no hits: " + string(body))
		}
		if len(hits.Array()) == 0 {
			RemovePitId(rootURI, pitId)
			break // we're done!
		}

		for _, hit := range hits.Array() {
			exported++
			progress.Add(int64(len(hit.Raw)))
			if _, err := fmt.Fprintln(w, hit.Raw); err != nil {
				RemovePitId(rootURI, pitId)
				return 0, err
			}
		}

		hitsArray := hits.Array()

		if (len(hitsArray) < 10000) {
			RemovePitId(rootURI, pitId)
			break // we're done!
		}
		lastItem := hitsArray[len(hitsArray) - 1]
		lastItemSort := gjson.Get(lastItem.Raw, "sort")
		uri := fmt.Sprintf("%s/_search?size=10000&sort=%s", rootURI, sortFieldString)
		postBodyWithSearchAfter, _ := sjson.Set(search, "search_after", lastItemSort.Array())
		postBody, _ := sjson.Set(postBodyWithSearchAfter, "pit", pitMap)
		req, err := http.NewRequest("POST", uri, strings.NewReader(postBody))
        req.Header.Add("Content-Type", "application/json")
		if err != nil {
			RemovePitId(rootURI, pitId)
			return 0, err
		}

		resp, err = client.Do(req)
		if err != nil {
			RemovePitId(rootURI, pitId)
			return 0, err
		}

		if resp.Body == nil {
			RemovePitId(rootURI, pitId)
			return 0, err
		}
	}
	progress.Done()
	return exported, nil
}
