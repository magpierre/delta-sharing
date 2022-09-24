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

	arrow "github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/apache/arrow/go/v9/parquet"
	"github.com/apache/arrow/go/v9/parquet/pqarrow"
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

/*
func LoadAsDataFrame(url string) (*dataframe.DataFrame, error) {
	pkg := "delta_sharing.go"
	fn := "LoadAsDataFrame"
	profile, shareStr, schemaStr, tableStr := _ParseURL(url)
	s, err := NewSharingClient(context.Background(), profile)
	if err != nil {
		return nil, err
	}
	t := table{Share: shareStr, Schema: schemaStr, Name: tableStr}
	lf, err := s.restClient.ListFilesInTable(t)
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
*/

func LoadAsArrowTable(url string, fileno int) (arrow.Table, error) {
	pkg := "delta_sharing.go"
	fn := "LoadAsArrowTable"
	profile, shareStr, schemaStr, tableStr := _ParseURL(url)
	s, err := NewSharingClient(context.Background(), profile, "")
	if err != nil {
		return nil, err
	}
	t := Table{Share: shareStr, Schema: schemaStr, Name: tableStr}
	lf, err := s.restClient.ListFilesInTable(t)
	if err != nil {
		return nil, err
	}

	if fileno > len(lf.AddFiles) || fileno < 0 {
		return nil, errors.New("Invalid index")
	}
	pf, err := s.restClient.readFileReader(lf.AddFiles[fileno].Url)
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

func LoadArrowTable(client interface{}, table Table, fileId string) (arrow.Table, error) {
	pkg := "delta_sharing.go"
	fn := "LoadArrowTable"
	if client == nil {
		return nil, &DSErr{pkg, fn, "client == nil", "Invalid client"}
	}

	f, err := client.(*sharingClient).restClient.ListFilesInTable(table)
	if err != nil {
		return nil, err
	}
	var urlValue *string

	for _, v := range f.AddFiles {
		if v.Id == fileId {
			urlValue = &v.Url
			break
		}
	}

	if urlValue == nil {
		return nil, &DSErr{pkg, fn, "v.Id == fileId", "fileid not found in table"}
	}

	pf, err := client.(*sharingClient).restClient.readFileReader(*urlValue)
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

type sharingClient struct {
	restClient *deltaSharingRestClient
}

func NewSharingClient(Ctx context.Context, ProfileFile string, cacheDir string) (*sharingClient, error) {
	pkg := "delta_sharing.go"
	fn := "NewSharingClient"
	p, err := newDeltaSharingProfile(ProfileFile)
	if err != nil {
		return nil, &DSErr{pkg, fn, "NewDeltaSharingProfile", err.Error()}
	}
	r := newDeltaSharingRestClient(Ctx, p, cacheDir, 5)
	if r == nil {
		return nil, &DSErr{pkg, fn, "NewDeltaSharingRestClient", "Could not create DeltaSharingRestClient"}
	}
	return &sharingClient{restClient: r}, err
}

func (s *sharingClient) ListShares() ([]share, error) {
	pkg := "delta_sharing.go"
	fn := "ListShares"
	sh, err := s.restClient.ListShares(0, "")
	if err != nil {
		return nil, &DSErr{pkg, fn, "s.RestClient.ListShares", err.Error()}
	}
	return sh.Shares, err
}

func (s *sharingClient) ListSchemas(share share) ([]schema, error) {
	sc, err := s.restClient.ListSchemas(share, 0, "")
	if err != nil {
		return nil, err
	}
	return sc.Schemas, err
}

func (s *sharingClient) ListTables(schema schema) ([]Table, error) {
	t, err := s.restClient.ListTables(schema, 0, "")
	if err != nil {
		return nil, err
	}
	return t.Tables, err
}

func (s *sharingClient) ListAllTables() ([]Table, error) {
	sh, err := s.restClient.ListShares(0, "")
	if err != nil {
		return nil, err
	}
	var tl []Table
	for _, v := range sh.Shares {
		x, err := s.restClient.ListAllTables(v, 0, "")
		if err != nil {
			return nil, err
		}
		tl = append(tl, x.Tables...)
	}
	return tl, err
}

func (s *sharingClient) ListFilesInTable(t Table) (*listFilesInTableResponse, error) {
	return s.restClient.ListFilesInTable(t)
}

func (s *sharingClient) GetTableVersion(t Table) (int, error) {
	v, err := s.restClient.QueryTableVersion(t)
	if err != nil {
		return -1, err
	}
	return v.DeltaTableVersion, nil
}

func (s *sharingClient) GetTableMetadata(t Table) (*metadata, error) {
	m, err := s.restClient.QueryTableMetadata(t)
	if err != nil {
		return nil, err
	}
	return &m.Metadata, nil
}

func (s *sharingClient) RemoveFileFromCache(url string) error {
	pkg := "delta_sharing.go"
	fn := "NewSharingClient"

	if s == nil || s.restClient == nil {
		return &DSErr{pkg, fn, "RemoveFileFromCache", "cache not initialized"}
	}
	return s.restClient.RemoveFileFromCache(url)
}

func (s *sharingClient) ListTableChanges(t Table, options CdfOptions) (*listCdcFilesResponse, error) {
	return s.restClient.ListTableChanges(t, options)
}
