/*
#
# Copyright (C) 2022 The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
*/

package delta_sharing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	afero "github.com/spf13/afero"
)

/* Response types */
type listSharesResponse struct {
	Shares []share
}
type listSchemasResponse struct {
	Schemas       []schema
	NextPageToken string
}
type listTablesResponse struct {
	Tables        []Table
	NextPageToken string
}
type listAllTablesResponse struct {
	Tables        []Table
	NextPageToken string
}
type queryTableMetadataReponse struct {
	Protocol protocol
	Metadata metadata
}
type queryTableVersionResponse struct {
	DeltaTableVersion int
}
type listFilesInTableResponse struct {
	Protocol protocol
	Metadata metadata
	AddFiles []File
}

type listCdcFilesResponse struct {
	Protocol protocol
	Metadata metadata
	Action   struct {
		Add    []File
		Cdc    []File
		Remove []File
	}
}

type deltaSharingRestClient struct {
	profile    *deltaSharingProfile
	numRetries int
	cacheDir   string
	cache      afero.Fs
	ctx        context.Context
}

/* Constructor for the DeltaSharingRestClient */
func newDeltaSharingRestClient(ctx context.Context, profile *deltaSharingProfile, cacheDir string, numRetries int) *deltaSharingRestClient {

	// create dir
	// with the right settings

	base := afero.NewOsFs()
	layer := afero.NewMemMapFs()
	ufs := afero.NewCacheOnReadFs(base, layer, 100*time.Second)
	var cache = cacheDir
	if len(cacheDir) == 0 {
		ufs.Mkdir("cache", 0755)
		cache = "cache"
	} else {
		ufs.Mkdir(cacheDir, 0755)
	}

	return &deltaSharingRestClient{
		profile:    profile,
		numRetries: numRetries,
		cacheDir:   cache,
		cache:      ufs,
		ctx:        ctx}

}

func (d *deltaSharingRestClient) RemoveFileFromCache(urlString string) error {
	u, err := url.Parse(urlString)
	if err != nil {
		return err
	}
	var completePath = d.cacheDir + "/" + u.Host + u.Path

	_, err = d.cache.Stat(completePath)
	if os.IsNotExist(err) {
		return nil
	}
	return d.cache.Remove(completePath)
}

func (d *deltaSharingRestClient) readFileReader(urlString string) (*bytes.Reader, error) {

	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}
	var completePath = d.cacheDir + "/" + u.Host + u.Path
	var p = strings.LastIndex(completePath, "/")
	_, err = d.cache.Stat(completePath[:p])
	if os.IsNotExist(err) {
		d.cache.MkdirAll(completePath[:p], 0755)
	}

	cs, err := d.cache.Stat(completePath)
	if os.IsNotExist(err) == false && cs.Size() == 0 {
		d.cache.Remove(completePath)
	} else if os.IsNotExist(err) == false && cs.Size() > 0 {
		f, err := d.cache.Open(completePath)
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(f)
		defer f.Close()
		br := bytes.NewReader(b)
		return br, err
	}

	f, err := d.cache.Create(completePath)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	r, err := http.Get(urlString)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	_, err = f.Write(b)
	br := bytes.NewReader(b)
	return br, err

}

func (d *deltaSharingRestClient) callSharingServer(request string) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "callSharingServer"
	var responses [][]byte
	rawUrl := d.profile.Endpoint + request
	urlval, _ := url.Parse(rawUrl)

	req := &http.Request{
		Method: "GET",
		URL:    urlval,
		Header: map[string][]string{
			"Content-Type":  {"application/json; charset=UTF-8"},
			"Authorization": {"Bearer " + d.profile.BearerToken},
		},
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
	}
	defer response.Body.Close()
	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &DSErr{pkg, fn, "io.ReadAll", err.Error()}
	}
	x := bytes.Split(bodyBytes, []byte{'\n'})
	for _, v := range x {
		if len(v) > 0 {
			responses = append(responses, v)
		}
	}
	return &responses, err
}
func (d *deltaSharingRestClient) callSharingServerWithParameters(request string, maxResult int, pageToken string) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "callSharingServerWithParameters"
	var responses [][]byte
	rawUrl := d.profile.Endpoint + request
	urlval, _ := url.Parse(rawUrl)
	req := &http.Request{
		Method: "GET",
		URL:    urlval,
		Header: map[string][]string{
			"Content-Type":  {"application/json; charset=UTF-8"},
			"Authorization": {"Bearer " + d.profile.BearerToken},
		},
	}
	var response *http.Response
	var retryCnt = 0
	var err error

	for {
		response, err = http.DefaultClient.Do(req)
		if err != nil {
			if retryCnt <= d.numRetries && d.shouldRetry(response) == true {
				retryCnt++
				continue
			}

			return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
		} else {
			break
		}
	}

	defer response.Body.Close()
	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &DSErr{pkg, fn, "io.ReadAll", err.Error()}
	}
	x := bytes.Split(bodyBytes, []byte{'\n'})
	for _, v := range x {
		if len(v) > 0 {
			responses = append(responses, v)
		}
	}

	return &responses, err
}

func (d *deltaSharingRestClient) getResponseHeader(request string) (map[string][]string, error) {
	pkg := "rest_client.go"
	fn := "getResponseHeader"
	url, err := url.Parse(d.profile.Endpoint + request)
	if err != nil {
		return nil, &DSErr{pkg, fn, "url.Parse", err.Error()}
	}
	req := &http.Request{
		Method: "HEAD",
		URL:    url,
		Header: map[string][]string{
			"Content-Type":  {"application/json; charset=UTF-8"},
			"Authorization": {"Bearer " + d.profile.BearerToken},
		},
	}
	var response *http.Response
	var retryCnt = 0

	for {
		response, err = http.DefaultClient.Do(req)
		if err != nil {
			if retryCnt <= d.numRetries && d.shouldRetry(response) == true {
				retryCnt++
				continue
			}

			return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
		} else {
			break
		}
	}
	return response.Header, err
}

func (c deltaSharingRestClient) ListShares(maxResult int, pageToken string) (*listSharesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListShares"
	// TODO Add support for parameters
	url := "/shares"

	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if rd == nil || len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "array returned is too short"}
	}
	var shares []share
	var share protoShare
	err = json.Unmarshal((*rd)[0], &share)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	shares = append(shares, share.Items...)
	return &listSharesResponse{Shares: shares}, err
}

func (c deltaSharingRestClient) ListSchemas(share share, maxResult int, pageToken string) (*listSchemasResponse, error) {
	pkg := "rest_client.go"
	fn := "ListSchemas"
	// TODO Add support for parameters
	url := "/shares/" + share.Name + "/schemas"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if rd == nil || len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "array returned is too short"}
	}
	var schemas []schema
	var schema protoSchema
	err = json.Unmarshal((*rd)[0], &schema)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	schemas = append(schemas, schema.Items...)
	return &listSchemasResponse{Schemas: schemas}, err
}

func (c deltaSharingRestClient) ListTables(schema schema, maxResult int, pageToken string) (*listTablesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListTables"
	url := "/shares/" + schema.Share + "/schemas/" + schema.Name + "/tables"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if rd == nil || len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "Invalid length of array"}
	}
	var tbl protoTable
	var tables []Table
	err = json.Unmarshal((*rd)[0], &tbl)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	tables = append(tables, tbl.Items...)
	return &listTablesResponse{Tables: tables}, err
}

func (c deltaSharingRestClient) ListAllTables(share share, maxResult int, pageToken string) (*listAllTablesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListAllTables"
	url := "/shares/" + share.Name + "/all-tables"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if rd == nil || len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "len(*rd)", "array returned is too short"}
	}
	var tables []Table
	var table protoTable

	for _, v := range (*rd)[0:] {
		err = json.Unmarshal(v, &table)
		if err != nil {
			return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
		}
		tables = append(tables, table.Items...)
	}
	return &listAllTablesResponse{Tables: tables}, err
}

func (c deltaSharingRestClient) QueryTableMetadata(table Table) (*queryTableMetadataReponse, error) {
	pkg := "rest_client.go"
	fn := "QueryTableMetadata"
	url := "/shares/" + table.Share + "/schemas/" + table.Schema + "/tables/" + table.Name + "/metadata"
	rd, err := c.callSharingServer(url)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServer", err.Error()}
	}
	var metadata protoMetadata
	var p protocol
	if len(*rd) != 2 {
		return nil, &DSErr{pkg, fn, "len(*rd)", ""}
	}
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}

	err = json.Unmarshal((*rd)[1], &metadata)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	return &queryTableMetadataReponse{Metadata: metadata.Metadata, Protocol: p}, err
}

func (c deltaSharingRestClient) QueryTableVersion(table Table) (*queryTableVersionResponse, error) {
	pkg := "rest_client.go"
	fn := "QueryTableVersion"
	rawUrl := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name
	r, err := c.getResponseHeader(rawUrl)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.getResponseHeader", err.Error()}
	}
	i, err := strconv.Atoi(r["Delta-Table-Version"][0])
	if err != nil {
		return nil, &DSErr{pkg, fn, "strconv.Atoi", err.Error()}
	}
	return &queryTableVersionResponse{DeltaTableVersion: i}, err
}

func (c *deltaSharingRestClient) ListFilesInTable(table Table) (*listFilesInTableResponse, error) {
	pkg := "rest_client.go"
	fn := "ListFilesInTable"
	url := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name + "/query"
	rd, err := c.postQuery(url, []string{""}, 0)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.postQuery", err.Error()}
	}
	if rd == nil || len(*rd) < 3 {
		return nil, &DSErr{pkg, fn, "len(*rd)", "Array returned is too short"}
	}
	var p protocol
	var m protoMetadata
	var f protoFile
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	err = json.Unmarshal((*rd)[1], &m)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	l := listFilesInTableResponse{Protocol: p, Metadata: m.Metadata}
	for _, v := range (*rd)[2:] {
		if len(v) == 0 {
			continue
		}
		err = json.Unmarshal(v, &f)
		if err != nil {
			return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
		}
		l.AddFiles = append(l.AddFiles, f.File)
	}
	return &l, err
}

func (c *deltaSharingRestClient) postQuery(request string, predicateHints []string, limitHint int) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "postQuery"
	// create request body
	rawURL := c.profile.Endpoint + "/" + request
	var responses [][]byte
	data := data{PredicateHints: predicateHints, LimitHint: limitHint}
	msg, err := json.Marshal(data)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Marshal", err.Error()}
	}
	reqBody := io.NopCloser(strings.NewReader(string(msg)))
	url, err := url.Parse(rawURL)
	if err != nil {
		return nil, &DSErr{pkg, fn, "url.Parse", err.Error()}
	}
	req := &http.Request{
		Method: "POST",
		URL:    url,
		Header: map[string][]string{
			"Content-Type":  {"application/json; charset=UTF-8"},
			"Authorization": {"Bearer " + c.profile.BearerToken},
		},
		Body: reqBody,
	}

	var response *http.Response
	var retryCnt = 0

	for {
		response, err = http.DefaultClient.Do(req)
		if err != nil {
			if retryCnt <= c.numRetries && c.shouldRetry(response) == true {
				retryCnt++
				continue
			}

			return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
		} else {
			break
		}
	}

	defer response.Body.Close()
	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &DSErr{pkg, fn, "io.ReadAll", err.Error()}
	}
	x := bytes.Split(bodyBytes, []byte{'\n'})
	for _, v := range x {
		responses = append(responses, v)
	}
	return &responses, err
}

func (c *deltaSharingRestClient) shouldRetry(r *http.Response) bool {

	if r == nil {
		fmt.Println("Retry connection due to error")
		return true
	}
	if r.StatusCode == 429 {
		fmt.Println("Retry operation due to status code: 429")
		return true
	} else if r.StatusCode >= 500 && r.StatusCode < 600 {
		fmt.Printf("Retry operation due to status code: %d\n", r.StatusCode)
		return true
	} else {
		return false
	}
}
