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
	"errors"

	"fmt"
	"strings"

	arrow "github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
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
	pkg := "delta_sharing.go"
	fn := "LoadAsDataFrame"
	profile, shareStr, schemaStr, tableStr := _ParseURL(url)
	s, err := NewSharingClient(context.Background(), profile)
	if err != nil {
		return nil, err
	}
	t := Table{Share: shareStr, Schema: schemaStr, Name: tableStr}
	lf, err := s.RestClient.ListFilesInTable(t)
	if err != nil || lf == nil {
		return nil, err
	}
	pf, err := http.NewHttpReader(lf.AddFiles[0].Url, false, false, map[string]string{})
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.NewHttpReader", err.Error()}
	}
	ctx := context.Background()
	df, err := imports.LoadFromParquet(ctx, pf)
	if err != nil {
		return nil, &DSErr{pkg, fn, "imports.LoadFromParquet", err.Error()}
	}
	return df, err
}

func LoadAsArrowTable(url string, fileno int) (arrow.Table, error) {
	pkg := "delta_sharing.go"
	fn := "LoadAsArrowTable"
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

	if fileno > len(lf.AddFiles) || fileno < 0 {
		return nil, errors.New("Invalid index")
	}
	pf, err := s.RestClient.ReadFileReader(lf.AddFiles[fileno].Url)
	if err != nil {
		return nil, err
	}
	mem := memory.NewGoAllocator()
	pa, err := pqarrow.ReadTable(context.Background(), pf, parquet.NewReaderProperties(nil), pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, &DSErr{pkg, fn, "pqarrow.ReadTable", err.Error()}
	}

	return pa, err
}

type SharingClient struct {
	Profile    *DeltaSharingProfile
	RestClient *DeltaSharingRestClient
}

func NewSharingClient(Ctx context.Context, ProfileFile string) (*SharingClient, error) {
	pkg := "delta_sharing.go"
	fn := "NewSharingClient"
	p, err := NewDeltaSharingProfile(ProfileFile)
	if err != nil || p == nil {
		return nil, &DSErr{pkg, fn, "NewDeltaSharingProfile", "Could not create DeltaSharingProfile"}
	}
	r := NewDeltaSharingRestClient(Ctx, p, 0)
	if r == nil {
		return nil, &DSErr{pkg, fn, "NewDeltaSharingRestClient", "Could not create DeltaSharingRestClient"}
	}
	return &SharingClient{Profile: p, RestClient: r}, err
}

func (s *SharingClient) ListShares() ([]Share, error) {
	pkg := "delta_sharing.go"
	fn := "ListShares"
	sh, err := s.RestClient.ListShares(0, "")
	if err != nil || sh == nil {
		return nil, &DSErr{pkg, fn, "s.RestClient.ListShares", "Could not list shares"}
	}
	return sh.Shares, err
}

func (s *SharingClient) ListSchemas(share Share) ([]Schema, error) {
	sc, err := s.RestClient.ListSchemas(share, 0, "")
	if err != nil || sc == nil {
		return nil, err
	}
	return sc.Schemas, err
}

func (s *SharingClient) ListTables(schema Schema) ([]Table, error) {
	t, err := s.RestClient.ListTables(schema, 0, "")
	if err != nil || t == nil {
		return nil, err
	}
	return t.Tables, err
}

func (s *SharingClient) ListAllTables() ([]Table, error) {
	sh, err := s.RestClient.ListShares(0, "")
	if err != nil || sh == nil {
		return nil, err
	}
	var tl []Table
	for _, v := range sh.Shares {
		x, err := s.RestClient.ListAllTables(v, 0, "")
		if err != nil || x == nil {
			return nil, err
		}
		tl = append(tl, x.Tables...)
	}
	return tl, err
}
