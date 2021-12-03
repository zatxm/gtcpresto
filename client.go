package gtcpresto

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type prestoClient struct {
	prestoCatalog string
	userAgent     string
	prestoUser    string
	prestoSchema  string

	nextUri          string
	infoUri          string
	partialCancelUri string
	state            string

	url     string
	result  *queryResult
	request string

	datas   []interface{}
	columns []string

	closed bool
}

type queryResult struct {
	ID               string        `json:"id"`
	InfoUri          string        `json:"infoUri"`
	NextUri          string        `json:"nextUri"`
	PartialCancelUri string        `json:"PartialCancelUri"`
	Data             []interface{} `json:"data"`
	Columns          []struct {
		Name string `json:"name"`
	} `json:"columns"`
	Error struct {
		ErrorCode   int `json:"errorCode"`
		FailureInfo struct {
			Message string `json:"message"`
		} `json:"failureInfo"`
	} `json:"error"`
	Stats struct {
		State           string `json:"state"`
		Scheduled       bool   `json:"scheduled"`
		CompletedSplits int    `json:"completedSplits"`
		TotalSplits     int    `json:"totalSplits"`
	} `json:"stats"`
}

func New(curl, catalog string) *prestoClient {
	return &prestoClient{
		prestoCatalog: catalog,
		userAgent:     userAgent,
		prestoUser:    prestoUser,
		prestoSchema:  prestoSchema,
		state:         stateInit,
		url:           curl,
		closed:        false}
}

func (p *prestoClient) NewQuery(request string) error {
	p.datas = []interface{}{}
	p.request = request
	p.nextUri = ""

	return p.getFirstQuery()
}

func (p *prestoClient) getFirstQuery() error {
	req, err := http.NewRequest("POST", p.url, strings.NewReader(p.request))
	if err != nil {
		return err
	}

	err = p.getPrestoRequest(req)
	if err != nil {
		return err
	}

	p.state = "RUNNING"

	return nil
}

func (p *prestoClient) GetData() []interface{} {
	if p.state != "FINISHED" {
		return nil
	}
	return p.datas
}

func (p *prestoClient) GetFinishedQuery() (map[string]interface{}, error) {
	q := p.url
	a := strings.Split(q, `/`)
	m := p.result.ID + `?pretty`
	a = append(a[:(len(a)-1)], `query`, m)
	q = strings.Join(a, `/`)
	req, _ := http.NewRequest("GET", q, nil)
	res, err := p.makeRequest(req)
	if err != nil {
		return nil, err
	}

	return p.readResultAll(res)
}

func (p *prestoClient) Columns() []string {
	return p.columns
}

func (p *prestoClient) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true
	req, _ := http.NewRequest("DELETE", p.nextUri, nil)
	res, err := p.makeRequest(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 204 {
		return fmt.Errorf("unexpected http status: %s", res.Status)
	}

	return nil
}

func (p *prestoClient) WaitQueryExec() error {
	p.getVarFromResult()

	for p.nextUri != "" {
		time.Sleep(initialRetry)
		err := p.getNextQuery()
		if err != nil {
			return err
		}
		p.getVarFromResult()
	}

	if p.state != "FINISHED" {
		return errors.New("Incoherent State at end of query")
	}

	return nil
}

func (p *prestoClient) getVarFromResult() {
	dat := p.result

	if dat.NextUri != "" {
		p.nextUri = dat.NextUri
	} else {
		p.nextUri = ""
	}

	if len(dat.Data) > 0 {
		p.datas = append(p.datas, dat.Data...)
	}

	if dat.InfoUri != "" {
		p.infoUri = dat.InfoUri
	}

	if dat.PartialCancelUri != "" {
		p.partialCancelUri = dat.PartialCancelUri
	}

	if dat.Stats.State != "" {
		p.state = dat.Stats.State
	}

	if len(p.columns) == 0 {
		p.columns = make([]string, len(dat.Columns))
		for i, col := range dat.Columns {
			p.columns[i] = col.Name
		}
	}
}

func (p *prestoClient) getNextQuery() error {
	req, _ := http.NewRequest("GET", p.nextUri, nil)
	return p.getPrestoRequest(req)
}

func (p *prestoClient) getPrestoRequest(req *http.Request) error {
	res, err := p.makeRequest(req)
	if err != nil {
		return err
	}

	result, err := p.readResult(res)
	if err != nil {
		return err
	}

	if result.Error.FailureInfo.Message != "" {
		return fmt.Errorf("query failed: %s", result.Error.FailureInfo.Message)
	}

	p.result = result

	return nil
}

func (p *prestoClient) makeRequest(req *http.Request) (*http.Response, error) {
	req.Header.Add("User-Agent", p.userAgent)
	req.Header.Add("X-Presto-User", p.prestoUser)
	req.Header.Add("X-Presto-Catalog", p.prestoCatalog)
	req.Header.Add("X-Presto-Schema", p.prestoSchema)

	//presto可能会返回503，这种情况重试
	retry := initialRetry
	for {
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		if res.StatusCode == 200 {
			return res, nil
		} else if res.StatusCode != 503 {
			return nil, errors.New(fmt.Sprintf("unexpected http status: %s", res.Status))
		}

		time.Sleep(retry)
		retry *= 2
		if retry > maxRetry {
			retry = maxRetry
		}
	}
	return nil, errors.New("Error MOMO")
}

func (p *prestoClient) readResultAll(resp *http.Response) (map[string]interface{}, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (p *prestoClient) readResult(resp *http.Response) (*queryResult, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	result := queryResult{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}
