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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type DSErr struct {
	Mod  string
	Func string
	Call string
	Msg  string
}

func (e *DSErr) Error() string {
	return fmt.Sprintf("\nErr: Mod:%s, Func:%s, Call:%s, Msg: %s\n", e.Mod, e.Func, e.Call, e.Msg)
}

/*
	DeltaSharingProfile Object
*/
type deltaSharingProfile struct {
	ShareCredentialsVersion int    `json:"shareCredentialsVersion"`
	Endpoint                string `json:"endpoint"`
	BearerToken             string `json:"bearerToken"`
	ExpirationTime          string `json:"expirationTime"`
}

func newDeltaSharingProfile(filename string) (*deltaSharingProfile, error) {
	pkg := "protocol.go"
	fn := "NewDeltaSharingProfile"
	d := deltaSharingProfile{}
	err := d.ReadFromFile(filename)
	if err != nil {
		return nil, &DSErr{pkg, fn, "d.ReadFromFile", err.Error()}
	}
	return &d, err
}

func (p *deltaSharingProfile) ReadFromFile(path string) error {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return err
	}
	msg, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	return json.Unmarshal(msg, p)
}

/*
	Protocol Object
*/
type protocol struct {
	Protocol struct {
		MinReaderVersion int32 `json:"minReaderVersion"`
	}
}

/*
	Format Object
*/
type format struct {
	Type   string   `json:"type"`
	Fields []string `json:"fields"`
}

/*
	Metadata Object
*/
type sparkField struct {
	Name     string
	Nullable bool
	Type     interface{}
	Metadata interface{}
}

type sparkSchema struct {
	Type   string
	Fields []sparkField
}

type metadata struct {
	Id               string `json:"id"`
	Name             string `json:"name"`
	Description      string `json:"description"`
	Format           format
	SchemaString     string   `json:"schemaString"`
	PartitionColumns []string `json:"partitionColumns"`
}

type protometadata struct {
	Metadata metadata
}

func (M *protometadata) GetSparkSchema() (*sparkSchema, error) {
	var sparkSchema sparkSchema
	err := json.Unmarshal([]byte(M.Metadata.SchemaString), &sparkSchema)
	if err != nil {
		return nil, err
	}
	return &sparkSchema, nil
}

/*
	File Object
*/
type protoFile struct {
	File file
}

type file struct {
	Url             string            `json:"url"`
	Id              string            `json:"id"`
	PartitionValues map[string]string `json:"partitionValues"`
	Size            float32           `json:"size"`
	Stats           string            `json:"stats"`
}

func (F *file) GetStats() (*stats, error) {
	var s stats
	if len(strings.Trim(F.Stats, " ")) == 0 {
		return nil, errors.New("Stats empty")
	}
	err := json.Unmarshal([]byte(F.Stats), &s)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

type stats struct {
	NumRecords int64
	MinValues  map[string]interface{}
	MaxValues  map[string]interface{}
	NullCount  map[string]interface{}
}

type protoShare struct {
	Items         []share
	NextPageToken string `json:"nextPageToken"`
}

type protoSchema struct {
	Items         []schema
	NextPageToken string `json:"nextPageToken"`
}

type protoTable struct {
	Items         []table
	NextPageToken string `json:"nextPageToken"`
}

type share struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

type schema struct {
	Name  string `json:"name"`
	Share string `json:"share"`
}

type table struct {
	Name   string `json:"name"`
	Share  string `json:"share"`
	Schema string `json:"schema"`
}

type data struct {
	PredicateHints []string `json:"predicateHints"`
	LimitHint      int      `json:"limitHint"`
}
