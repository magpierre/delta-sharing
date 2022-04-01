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
	"io/ioutil"
	"log"
	"os"
)

/*
	DeltaSharingProfile Object
*/
type DeltaSharingProfile struct {
	ShareCredentialsVersion int    `json:"shareCredentialsVersion"`
	Endpoint                string `json:"endpoint"`
	BearerToken             string `json:"bearerToken"`
}

func NewDeltaSharingProfile(filename string) *DeltaSharingProfile {
	d := DeltaSharingProfile{}
	err := d.ReadFromFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	x, err := json.MarshalIndent(d, "", "    ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("response: %+v\n", string(x))
	return &d
}

func (p *DeltaSharingProfile) ReadFromFile(path string) error {
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
type Protocol struct {
	Protocol struct {
		MinReaderVersion int32 `json:"minReaderVersion"`
	}
}

/*
	Format Object
*/
type Format struct {
	Type   string   `json:"type"`
	Fields []string `json:"fields"`
}

/*
	Metadata Object
*/

type Metadata struct {
	Metadata struct {
		Id               string `json:"id"`
		Name             string `json:"name"`
		Description      string `json:"description"`
		Format           Format
		SchemaString     string   `json:"schemaString"`
		PartitionColumns []string `json:"partitionColumns"`
	}
}

/*
	File Object
*/
type ProtoFile struct {
	File File
}

type File struct {
	Url             string            `json:"url"`
	Id              string            `json:"id"`
	PartitionValues map[string]string `json:"partitionValues"`
	Size            float32           `json:"size"`
	Stats           string            `json:"stats"`
}

type ProtoShare struct {
	Items         []Share
	NextPageToken string `json:"nextPageToken"`
}

type ProtoSchema struct {
	Items         []Schema
	NextPageToken string `json:"nextPageToken"`
}

type ProtoTable struct {
	Items         []Table
	NextPageToken string `json:"nextPageToken"`
}

type Share struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

type Schema struct {
	Name  string `json:"name"`
	Share string `json:"share"`
}

type Table struct {
	Name   string `json:"name"`
	Share  string `json:"share"`
	Schema string `json:"schema"`
}
