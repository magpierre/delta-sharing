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
	"context"
	"fmt"
	"strings"

	"github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/imports"
	"github.com/xitongsys/parquet-go-source/http"
)

func _ParseURL(url string) (string, string, string, string) {
	i := strings.LastIndex(url, "#")
	if i < 0 {
		fmt.Println("Invalid URL:", url)
		return "", "", "", ""
	}
	profile := url[0:i]
	fragments := strings.Split(url[i+1:], ".")
	if len(fragments) != 3 {
		fmt.Println("Invalid URL:", url)
		return "", "", "", ""
	}
	share := strings.Trim(fragments[0], " ")
	schema := strings.Trim(fragments[1], " ")
	table := strings.Trim(fragments[2], " ")
	if len(share) == 0 || len(schema) == 0 || len(table) == 0 {
		fmt.Println("Invalid URL:", url)
		return "", "", "", ""
	}
	return profile, share, schema, table
}

func LoadAsDataFrame(url string) (*dataframe.DataFrame, error) {
	profile, shareStr, schemaStr, tableStr := _ParseURL(url)
	s, err := NewSharingClient(context.Background(), profile)
	if err != nil {
		return nil, err
	}
	t := Table{Share: shareStr, Schema: schemaStr, Name: tableStr}
	lf, err := s.RestClient.ListFilesInTable(t)
	if err != nil {
		return nil, err
	}
	parquetFile, err := http.NewHttpReader(lf.AddFiles[0].Url, false, false, map[string]string{})
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "delta_sharing.go",
			Method:       "LoadAsDataFrame",
			Operation:    "http.NewHttpReader(lf.AddFiles[0].Url, false, false, map[string]string{})",
			ErrorMessage: err.Error(),
		}
	}
	ctx := context.Background()
	df, err := imports.LoadFromParquet(ctx, parquetFile)
	if err != nil {
		return nil, &DeltaSharingError{
			Module:       "delta_sharing.go",
			Method:       "LoadAsDataFrame",
			Operation:    "imports.LoadFromParquet(ctx, parquetFile)",
			ErrorMessage: err.Error(),
		}
	}
	return df, err
}

type SharingClient struct {
	Profile    *DeltaSharingProfile
	RestClient *DeltaSharingRestClient
}

func NewSharingClient(Ctx context.Context, ProfileFile string) (*SharingClient, error) {
	p, err := NewDeltaSharingProfile(ProfileFile)
	if err != nil {
		return nil, err
	}
	r := NewDeltaSharingRestClient(Ctx, p, 0)
	return &SharingClient{Profile: p, RestClient: r}, err
}

func (s *SharingClient) ListShares() ([]Share, error) {
	shares, err := s.RestClient.ListShares(0, "")
	return shares.Shares, err
}

func (s *SharingClient) ListSchemas(share Share) ([]Schema, error) {
	schemas, err := s.RestClient.ListSchemas(share, 0, "")
	return schemas.Schemas, err
}

func (s *SharingClient) ListTables(schema Schema) ([]Table, error) {
	tables, err := s.RestClient.ListTables(schema, 0, "")
	return tables.Tables, err
}

func (s *SharingClient) ListAllTables() ([]Table, error) {
	shares, err := s.RestClient.ListShares(0, "")
	if err != nil {
		return nil, err
	}
	var ctl []Table
	for _, v := range shares.Shares {
		x, err := s.RestClient.ListAllTables(v, 0, "")
		if err != nil {
			return nil, err
		}
		ctl = append(ctl, x.Tables...)
	}
	return ctl, err
}
