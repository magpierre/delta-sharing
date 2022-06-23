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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

/* Response types */
type ListSharesResponse struct {
	Shares []Share
}
type ListSchemasResponse struct {
	Schemas       []Schema
	NextPageToken string
}
type ListTablesResponse struct {
	Tables        []Table
	NextPageToken string
}
type ListAllTablesResponse struct {
	Tables        []Table
	NextPageToken string
}
type QueryTableMetadataReponse struct {
	Protocol Protocol
	Metadata Metadata
}
type QueryTableVersionResponse struct {
	DeltaTableVersion int
}
type ListFilesInTableResponse struct {
	Protocol Protocol
	Metadata Metadata
	AddFiles []File
}

type DeltaSharingRestClient struct {
	Profile    *DeltaSharingProfile
	NumRetries int
	Ctx        context.Context
}

/* Constructor for the DeltaSharingRestClient */
func NewDeltaSharingRestClient(ctx context.Context, profile *DeltaSharingProfile, numRetries int) *DeltaSharingRestClient {
	return &DeltaSharingRestClient{Profile: profile, NumRetries: numRetries, Ctx: ctx}
}

func (d *DeltaSharingRestClient) ReadFileReader(url string) (*bytes.Reader, error) {

	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(r.Body)
	br := bytes.NewReader(b)

	return br, err

}

func (d *DeltaSharingRestClient) callSharingServer(request string) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "callSharingServer"
	var responses [][]byte
	rawUrl := d.Profile.Endpoint + request
	urlval, _ := url.Parse(rawUrl)

	req := &http.Request{
		Method: "GET",
		URL:    urlval,
		Header: map[string][]string{
			"Content-Type":  {"application/json; charset=UTF-8"},
			"Authorization": {"Bearer " + d.Profile.BearerToken},
		},
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
	}
	defer response.Body.Close()
	s := bufio.NewScanner(response.Body)
	for s.Scan() {
		responses = append(responses, s.Bytes())
	}
	if err := s.Err(); err != nil {
		return nil, &DSErr{pkg, fn, "s.Scan", err.Error()}
	}
	return &responses, err
}
func (d *DeltaSharingRestClient) callSharingServerWithParameters(request string, maxResult int, pageToken string) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "callSharingServerWithParameters"
	var responses [][]byte
	rawUrl := d.Profile.Endpoint + request
	urlval, _ := url.Parse(rawUrl)
	req := &http.Request{
		Method: "GET",
		URL:    urlval,
		Header: map[string][]string{
			"Content-Type":  {"application/json; charset=UTF-8"},
			"Authorization": {"Bearer " + d.Profile.BearerToken},
		},
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
	}
	defer response.Body.Close()
	s := bufio.NewScanner(response.Body)
	for s.Scan() {
		responses = append(responses, s.Bytes())
	}
	if err := s.Err(); err != nil {
		return nil, &DSErr{pkg, fn, "s.Scan", err.Error()}
	}
	return &responses, err
}

func (d *DeltaSharingRestClient) getResponseHeader(request string) (map[string][]string, error) {
	pkg := "rest_client.go"
	fn := "getResponseHeader"
	url, err := url.Parse(d.Profile.Endpoint + request)
	if err != nil {
		return nil, &DSErr{pkg, fn, "url.Parse", err.Error()}
	}
	req := &http.Request{
		Method: "HEAD",
		URL:    url,
		Header: map[string][]string{
			"Content-Type":  {"application/json; charset=UTF-8"},
			"Authorization": {"Bearer " + d.Profile.BearerToken},
		},
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
	}
	return response.Header, err
}

func (c DeltaSharingRestClient) ListShares(maxResult int, pageToken string) (*ListSharesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListShares"
	// TODO Add support for parameters
	url := "/shares"

	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "array returned is too short"}
	}
	var shares []Share
	var share ProtoShare
	err = json.Unmarshal((*rd)[0], &share)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	shares = append(shares, share.Items...)
	return &ListSharesResponse{Shares: shares}, err
}

func (c DeltaSharingRestClient) ListSchemas(share Share, maxResult int, pageToken string) (*ListSchemasResponse, error) {
	pkg := "rest_client.go"
	fn := "ListSchemas"
	// TODO Add support for parameters
	url := "/shares/" + share.Name + "/schemas"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "array returned is too short"}
	}
	var schemas []Schema
	var schema ProtoSchema
	err = json.Unmarshal((*rd)[0], &schema)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	schemas = append(schemas, schema.Items...)
	return &ListSchemasResponse{Schemas: schemas}, err
}

func (c DeltaSharingRestClient) ListTables(schema Schema, maxResult int, pageToken string) (*ListTablesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListTables"
	url := "/shares/" + schema.Share + "/schemas/" + schema.Name + "/tables"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "Invalid length of array"}
	}
	var table ProtoTable
	var tables []Table
	err = json.Unmarshal((*rd)[0], &table)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	tables = append(tables, table.Items...)
	return &ListTablesResponse{Tables: tables}, err
}

func (c DeltaSharingRestClient) ListAllTables(share Share, maxResult int, pageToken string) (*ListAllTablesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListAllTables"
	url := "/shares/" + share.Name + "/all-tables"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if len(*rd) < 2 {
		return nil, &DSErr{pkg, fn, "len(*rd)", "array returned is too short"}
	}
	var tables []Table
	var p Protocol
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	for _, v := range (*rd)[1:] {
		table := Table{}
		err = json.Unmarshal(v, &table)
		if err != nil {
			return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
		}
		tables = append(tables, table)
	}
	return &ListAllTablesResponse{Tables: tables}, err
}

func (c DeltaSharingRestClient) QueryTableMetadata(table Table) (*QueryTableMetadataReponse, error) {
	pkg := "rest_client.go"
	fn := "QueryTableMetadata"
	url := "/shares/" + table.Share + "/schemas/" + table.Schema + "/tables/" + table.Name + "/metadata"
	rd, err := c.callSharingServer(url)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServer", err.Error()}
	}
	var metadata Metadata
	var p Protocol
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
	return &QueryTableMetadataReponse{Metadata: metadata, Protocol: p}, err
}

func (c DeltaSharingRestClient) QueryTableVersion(table Table) (*QueryTableVersionResponse, error) {
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
	return &QueryTableVersionResponse{DeltaTableVersion: i}, err
}

func (c *DeltaSharingRestClient) ListFilesInTable(table Table) (*ListFilesInTableResponse, error) {
	pkg := "rest_client.go"
	fn := "ListFilesInTable"
	url := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name + "/query"
	rd, err := c.postQuery(url, []string{""}, 0)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.postQuery", err.Error()}
	}
	if len(*rd) < 3 {
		return nil, &DSErr{pkg, fn, "len(*rd)", "Array returned is too short"}
	}
	var p Protocol
	var m Metadata
	var f ProtoFile
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	err = json.Unmarshal((*rd)[1], &m)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	l := ListFilesInTableResponse{Protocol: p, Metadata: m}
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

func (c *DeltaSharingRestClient) postQuery(request string, predicateHints []string, limitHint int) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "postQuery"
	// create request body
	rawURL := c.Profile.Endpoint + "/" + request
	var responses [][]byte
	data := Data{PredicateHints: predicateHints, LimitHint: limitHint}
	msg, err := json.Marshal(data)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Marshal", err.Error()}
	}
	reqBody := ioutil.NopCloser(strings.NewReader(string(msg)))
	url, err := url.Parse(rawURL)
	if err != nil {
		return nil, &DSErr{pkg, fn, "url.Parse", err.Error()}
	}
	req := &http.Request{
		Method: "POST",
		URL:    url,
		Header: map[string][]string{
			"Content-Type":  {"application/json; charset=UTF-8"},
			"Authorization": {"Bearer " + c.Profile.BearerToken},
		},
		Body: reqBody,
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
	}
	defer response.Body.Close()
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, &DSErr{pkg, fn, "ioutil.ReadAll", err.Error()}
	}
	x := bytes.Split(bodyBytes, []byte{'\n'})
	for _, v := range x {
		responses = append(responses, v)
	}
	return &responses, err
}
