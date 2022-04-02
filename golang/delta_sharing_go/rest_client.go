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

func (d *DeltaSharingRestClient) callSharingServer(request string) (*[][]byte, error) {
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
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "callSharingServer",
			Operation:    "http.DefaultClient.Do(req)",
			ErrorMessage: err.Error(),
		}
	}
	s := bufio.NewScanner(response.Body)
	for s.Scan() {
		responses = append(responses, s.Bytes())
	}
	if err := s.Err(); err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "callSharingServer",
			Operation:    "s.Scan()",
			ErrorMessage: err.Error(),
		}
	}
	return &responses, err
}
func (d *DeltaSharingRestClient) callSharingServerWithParameters(request string, maxResult int, pageToken string) (*[][]byte, error) {
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
	/*
		reqDump, err := httputil.DumpRequestOut(req, true)
		if err != nil {
			log.Println(err)
		}

		log.Printf("REQUEST:\n%s", string(reqDump))
	*/
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "callSharingServerWithParameters",
			Operation:    "http.DefaultClient.Do(req)",
			ErrorMessage: err.Error(),
		}
	}
	/*
		respDump, err := httputil.DumpResponse(response, true)
		if err != nil {
			log.Println(err)
		}

		log.Printf("RESPONSE:\n%s", string(respDump))
	*/

	s := bufio.NewScanner(response.Body)
	for s.Scan() {
		responses = append(responses, s.Bytes())
	}
	if err := s.Err(); err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "callSharingServerWithParameters",
			Operation:    "s.Scan()",
			ErrorMessage: err.Error(),
		}
	}
	return &responses, err
}

func (d *DeltaSharingRestClient) getResponseHeader(request string) (map[string][]string, error) {
	url, err := url.Parse(d.Profile.Endpoint + request)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "getResponseHeader",
			Operation:    "url.Parse(d.Profile.Endpoint + request)",
			ErrorMessage: err.Error(),
		}
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
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "getResponseHeader",
			Operation:    "http.DefaultClient.Do(req)",
			ErrorMessage: err.Error(),
		}
	}
	return response.Header, err
}

func (c DeltaSharingRestClient) ListShares(maxResult int, pageToken string) (*ListSharesResponse, error) {
	// TODO Add support for parameters
	url := "/shares"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListShares",
			Operation:    "c.callSharingServerWithParameters(url, maxResult, pageToken)",
			ErrorMessage: err.Error(),
		}
	}
	if len(*rd) < 1 {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListShares",
			Operation:    "c.callSharingServerWithParameters(url, maxResult, pageToken)",
			ErrorMessage: "array returned is too short",
		}
	}
	var shares []Share
	var share ProtoShare
	err = json.Unmarshal((*rd)[0], &share)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListShares",
			Operation:    "json.Unmarshal((*rd)[0], &share))",
			ErrorMessage: err.Error(),
		}
	}
	shares = append(shares, share.Items...)
	return &ListSharesResponse{Shares: shares}, err
}

func (c DeltaSharingRestClient) ListSchemas(share Share, maxResult int, pageToken string) (*ListSchemasResponse, error) {
	// TODO Add support for parameters
	url := "/shares/" + share.Name + "/schemas"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListSchemas",
			Operation:    "c.callSharingServerWithParameters(url, maxResult, pageToken)",
			ErrorMessage: err.Error(),
		}
	}
	if len(*rd) < 1 {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListSchemas",
			Operation:    "c.callSharingServerWithParameters(url, maxResult, pageToken)",
			ErrorMessage: "array returned is too short",
		}
	}
	var schemas []Schema
	var schema ProtoSchema
	err = json.Unmarshal((*rd)[0], &schema)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListSchemas",
			Operation:    "json.Unmarshal((*rd)[0], &schema)",
			ErrorMessage: err.Error(),
		}
	}
	schemas = append(schemas, schema.Items...)
	return &ListSchemasResponse{Schemas: schemas}, err
}

func (c DeltaSharingRestClient) ListTables(schema Schema, maxResult int, pageToken string) (*ListTablesResponse, error) {
	url := "/shares/" + schema.Share + "/schemas/" + schema.Name + "/tables"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListTables",
			Operation:    "c.callSharingServerWithParameters(url, maxResult, pageToken)",
			ErrorMessage: err.Error(),
		}
	}
	if len(*rd) < 1 {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListTables",
			Operation:    "c.callSharingServerWithParameters(url, maxResult, pageToken)",
			ErrorMessage: "Invalid length of array",
		}
	}
	var table ProtoTable
	var tables []Table
	err = json.Unmarshal((*rd)[0], &table)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListTables",
			Operation:    "json.Unmarshal((*rd)[0], &table)",
			ErrorMessage: err.Error(),
		}
	}
	tables = append(tables, table.Items...)
	return &ListTablesResponse{Tables: tables}, err
}

func (c DeltaSharingRestClient) ListAllTables(share Share, maxResult int, pageToken string) (*ListAllTablesResponse, error) {
	url := "/shares/" + share.Name + "/all-tables"
	rd, err := c.callSharingServerWithParameters(url, maxResult, pageToken)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListAllTables",
			Operation:    "c.callSharingServerWithParameters(url, maxResult, pageToken)",
			ErrorMessage: err.Error(),
		}
	}
	if len(*rd) < 2 {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListAllTables",
			Operation:    "len(*rd) < 2",
			ErrorMessage: "array returned is too short",
		}
	}
	var tables []Table
	var p Protocol
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListAllTables",
			Operation:    "json.Unmarshal((*rd)[0], &p)",
			ErrorMessage: err.Error(),
		}
	}
	for _, v := range (*rd)[1:] {
		table := Table{}
		err = json.Unmarshal(v, &table)
		if err != nil {
			return nil, &DeltaSharingError{
				Module:       "rest_client.go",
				Method:       "ListAllTables",
				Operation:    "json.Unmarshal(v, &table)",
				ErrorMessage: err.Error(),
			}
		}
		tables = append(tables, table)
	}
	return &ListAllTablesResponse{Tables: tables}, err
}

func (c DeltaSharingRestClient) QueryTableMetadata(table Table) (*QueryTableMetadataReponse, error) {
	url := "/shares/" + table.Share + "/schemas/" + table.Schema + "/tables/" + table.Name + "/metadata"
	rd, err := c.callSharingServer(url)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "QueryTableMetadata",
			Operation:    "c.callSharingServer(url)",
			ErrorMessage: err.Error(),
		}
	}
	var metadata Metadata
	var p Protocol
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "QueryTableMetadata",
			Operation:    "json.Unmarshal((*rd)[0], &p)",
			ErrorMessage: err.Error(),
		}
	}
	err = json.Unmarshal((*rd)[1], &metadata)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "QueryTableMetadata",
			Operation:    "json.Unmarshal((*rd)[1], &metadata)",
			ErrorMessage: err.Error(),
		}
	}
	return &QueryTableMetadataReponse{Metadata: metadata, Protocol: p}, err
}

func (c DeltaSharingRestClient) QueryTableVersion(table Table) (*QueryTableVersionResponse, error) {
	rawUrl := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name
	r, err := c.getResponseHeader(rawUrl)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "QueryTableVersion",
			Operation:    "c.getResponseHeader(rawUrl)",
			ErrorMessage: err.Error(),
		}
	}
	i, err := strconv.Atoi(r["Delta-Table-Version"][0])
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "QueryTableVersion",
			Operation:    "strconv.Atoi(r[\"Delta-Table-Version\"][0])",
			ErrorMessage: err.Error(),
		}
	}
	return &QueryTableVersionResponse{DeltaTableVersion: i}, err
}

func (c *DeltaSharingRestClient) ListFilesInTable(table Table) (*ListFilesInTableResponse, error) {
	url := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name + "/query"
	rd, err := c.postQuery(url, []string{""}, 0)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListFilesInTable",
			Operation:    "c.postQuery(url, []string{\"\"}, 0)",
			ErrorMessage: err.Error(),
		}
	}
	if len(*rd) < 3 {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListFilesInTable",
			Operation:    "len(*rd) < 3",
			ErrorMessage: "Array returned is too short",
		}
	}
	var p Protocol
	var m Metadata
	var f ProtoFile
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListFilesInTable",
			Operation:    "json.Unmarshal((*rd)[0], &p)",
			ErrorMessage: err.Error(),
		}
	}
	err = json.Unmarshal((*rd)[1], &m)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "ListFilesInTable",
			Operation:    "json.Unmarshal((*rd)[1], &m)",
			ErrorMessage: err.Error(),
		}
	}
	l := ListFilesInTableResponse{Protocol: p, Metadata: m}
	for _, v := range (*rd)[2:] {
		if len(v) == 0 {
			continue
		}
		err = json.Unmarshal(v, &f)
		if err != nil {
			return nil, &DeltaSharingError{
				Module:       "rest_client.go",
				Method:       "ListFilesInTable",
				Operation:    "json.Unmarshal(v, &f)",
				ErrorMessage: err.Error(),
			}
		}
		l.AddFiles = append(l.AddFiles, f.File)
	}
	return &l, err
}

func (c *DeltaSharingRestClient) postQuery(request string, predicateHints []string, limitHint int) (*[][]byte, error) {
	// create request body
	rawURL := c.Profile.Endpoint + "/" + request
	var responses [][]byte
	data := Data{PredicateHints: predicateHints, LimitHint: limitHint}
	msg, err := json.Marshal(data)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "postQuery",
			Operation:    "json.Marshal(data)",
			ErrorMessage: err.Error(),
		}
	}
	reqBody := ioutil.NopCloser(strings.NewReader(string(msg)))
	url, err := url.Parse(rawURL)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "postQuery",
			Operation:    "url.Parse(rawURL)",
			ErrorMessage: err.Error(),
		}
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
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "postQuery",
			Operation:    "http.DefaultClient.Do(req)",
			ErrorMessage: err.Error(),
		}
	}
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "rest_client.go",
			Method:       "postQuery",
			Operation:    "ioutil.ReadAll(response.Body)",
			ErrorMessage: err.Error(),
		}
	}
	x := bytes.Split(bodyBytes, []byte{'\n'})
	for _, v := range x {
		responses = append(responses, v)
	}
	return &responses, err
}
