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
	"context"
	"encoding/json"
	"fmt"

	"io/ioutil"
	"log"
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

func (d *DeltaSharingRestClient) callSharingServer(request string) [][]byte {
	var responses [][]byte
	url := d.Profile.Endpoint + request
	response, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
	}
	s := bufio.NewScanner(response.Body)
	for s.Scan() {
		responses = append(responses, s.Bytes())
	}
	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
	return responses
}
func (d *DeltaSharingRestClient) callSharingServerWithParameters(request string, maxResult int, pageToken string) [][]byte {
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

	//response, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
	}
	s := bufio.NewScanner(response.Body)
	for s.Scan() {
		responses = append(responses, s.Bytes())
	}
	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
	return responses
}

func (d *DeltaSharingRestClient) getResponseHeader(request string) map[string][]string {
	url, err := url.Parse(d.Profile.Endpoint + request)
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
		log.Fatal("Error:", err)
	}
	fmt.Println(response)
	return response.Header
}

func (c DeltaSharingRestClient) ListShares(maxResult int, pageToken string) ListSharesResponse {
	// TODO Add support for parameters
	url := "/shares"
	rd := c.callSharingServerWithParameters(url, maxResult, pageToken)

	var shares []Share
	var share ProtoShare
	err := json.Unmarshal(rd[0], &share)
	if err != nil {
		fmt.Println(err)
	}
	shares = append(shares, share.Items...)

	return ListSharesResponse{Shares: shares}

}

func (c DeltaSharingRestClient) ListSchemas(share Share, maxResult int, pageToken string) ListSchemasResponse {
	// TODO Add support for parameters
	url := "/shares/" + share.Name + "/schemas"
	rd := c.callSharingServerWithParameters(url, maxResult, pageToken)
	var schemas []Schema
	var schema ProtoSchema
	err := json.Unmarshal(rd[0], &schema)
	if err != nil {
		fmt.Println(err)
	}
	schemas = append(schemas, schema.Items...)

	return ListSchemasResponse{Schemas: schemas}
}

func (c DeltaSharingRestClient) ListTables(schema Schema, maxResult int, pageToken string) ListTablesResponse {
	url := "/shares/" + schema.Share + "/schemas/" + schema.Name + "/tables"
	rd := c.callSharingServerWithParameters(url, maxResult, pageToken)
	var table ProtoTable
	var tables []Table
	err := json.Unmarshal(rd[0], &table)
	if err != nil {
		fmt.Println(err)
	}
	tables = append(tables, table.Items...)

	return ListTablesResponse{Tables: tables}
}

func (c DeltaSharingRestClient) ListAllTables(share Share, maxResult int, pageToken string) ListAllTablesResponse {
	url := "/shares/" + share.Name + "/all-tables"
	rd := c.callSharingServerWithParameters(url, maxResult, pageToken)
	var tables []Table
	var p Protocol
	err := json.Unmarshal(rd[0], &p)
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range rd[1:] {
		table := Table{}
		err = json.Unmarshal(v, &table)
		if err != nil {
			log.Fatal(err)
		}
		tables = append(tables, table)
	}

	return ListAllTablesResponse{Tables: tables}
}

func (c DeltaSharingRestClient) QueryTableMetadata(table Table) QueryTableMetadataReponse {
	url := "/shares/" + table.Share + "/schemas/" + table.Schema + "/tables/" + table.Name + "/metadata"
	rd := c.callSharingServer(url)
	var metadata Metadata
	var p Protocol
	err := json.Unmarshal(rd[0], &p)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(rd[1], &metadata)
	if err != nil {
		log.Fatal(err)
	}

	return QueryTableMetadataReponse{Metadata: metadata, Protocol: p}
}

func (c DeltaSharingRestClient) QueryTableVersion(table Table) QueryTableVersionResponse {
	rawUrl := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name
	r := c.getResponseHeader(rawUrl)
	i, err := strconv.Atoi(r["Delta-Table-Version"][0])
	if err != nil {
		log.Fatal(err)
	}
	return QueryTableVersionResponse{DeltaTableVersion: i}
}

func (c *DeltaSharingRestClient) ListFilesInTable(table Table) ListFilesInTableResponse {
	url := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name + "/query"
	rd := c.postQuery(url, nil, nil)

	var p Protocol
	var m Metadata
	var f ProtoFile

	err := json.Unmarshal(rd[0], &p)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(rd[1], &m)
	if err != nil {
		log.Fatal(err)
	}

	l := ListFilesInTableResponse{Protocol: p, Metadata: m}

	for _, v := range rd[2:] {
		err = json.Unmarshal(v, &f)
		if err != nil {
			log.Fatal(err)
		}

		l.AddFiles = append(l.AddFiles, f.File)
	}
	return l
}

func (c *DeltaSharingRestClient) postQuery(request string, predicateHints *[]string, limitHint *int) [][]byte {
	// create request body
	rawURL := c.Profile.Endpoint + "/" + request
	var responses [][]byte
	reqBody := ioutil.NopCloser(strings.NewReader(`
		{
			"predicateHints": [],
			"limitHint": 0
		}
	`))

	url, err := url.Parse(rawURL)
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
		log.Fatal("Error:", err)
	}

	// Scan the body and split the body into an array of responses
	s := bufio.NewScanner(response.Body)
	for s.Scan() {
		responses = append(responses, s.Bytes())
	}
	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
	return responses
}
