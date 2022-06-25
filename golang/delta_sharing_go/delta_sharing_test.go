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
	"reflect"
	"testing"

	arrow "github.com/apache/arrow/go/v8/arrow"
)

func Test_ParseURL(t *testing.T) {
	type args struct {
		url string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
		want2 string
		want3 string
	}{
		{
			name: "test1",
			args: args{
				url: "/config1.share#share_32a3aac80cbe.abcd.abcd_2018",
			},
			want:  "/config1.share",
			want1: "share_32a3aac80cbe",
			want2: "abcd",
			want3: "abcd_2018",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2, got3 := _ParseURL(tt.args.url)
			if got != tt.want {
				t.Errorf("_ParseURL() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("_ParseURL() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("_ParseURL() got2 = %v, want %v", got2, tt.want2)
			}
			if got3 != tt.want3 {
				t.Errorf("_ParseURL() got3 = %v, want %v", got3, tt.want3)
			}
		})
	}
}

func TestLoadAsArrowTable(t *testing.T) {
	type args struct {
		url    string
		fileno int
	}
	tests := []struct {
		name    string
		args    args
		want    arrow.Table
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadAsArrowTable(tt.args.url, tt.args.fileno)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAsArrowTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadAsArrowTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSharingClient(t *testing.T) {
	type args struct {
		Ctx         context.Context
		ProfileFile string
	}
	tests := []struct {
		name    string
		args    args
		want    *sharingClient
		wantErr bool
	}{
		{
			name: "test1",
			args: args{context.Background(), "/Users/magnuspierre/Documents/shares/open-datasets.share.txt"},
			want: &sharingClient{
				restClient: &deltaSharingRestClient{
					profile: &deltaSharingProfile{
						ShareCredentialsVersion: 1,
						Endpoint:                "https://sharing.delta.io/delta-sharing/",
						BearerToken:             "faaie590d541265bcab1f2de9813274bf233",
						ExpirationTime:          "",
					},
					numRetries: 5,
					ctx:        context.Background(),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSharingClient(tt.args.Ctx, tt.args.ProfileFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSharingClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(*got, *tt.want) {
				t.Log(&got)
				t.Log(&tt.want)
				t.Errorf("NewSharingClient() = %v, want %v", *got, *tt.want)
			}
		})
	}
}

func TestSharingClient_ListShares(t *testing.T) {
	type fields struct {
		RestClient *deltaSharingRestClient
	}
	tests := []struct {
		name    string
		fields  fields
		want    []share
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				RestClient: &deltaSharingRestClient{
					profile: &deltaSharingProfile{
						ShareCredentialsVersion: 1,
						Endpoint:                "https://sharing.delta.io/delta-sharing/",
						BearerToken:             "faaie590d541265bcab1f2de9813274bf233",
						ExpirationTime:          "",
					},
				}},
			want: []share{
				{
					Name: "delta_sharing",
					Id:   "",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sharingClient{
				restClient: tt.fields.RestClient,
			}
			got, err := s.ListShares()
			if (err != nil) != tt.wantErr {
				t.Errorf("SharingClient.ListShares() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SharingClient.ListShares() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSharingClient_ListSchemas(t *testing.T) {
	type fields struct {
		RestClient *deltaSharingRestClient
	}
	type args struct {
		share share
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []schema
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				RestClient: &deltaSharingRestClient{
					profile: &deltaSharingProfile{
						ShareCredentialsVersion: 1,
						Endpoint:                "https://sharing.delta.io/delta-sharing/",
						BearerToken:             "faaie590d541265bcab1f2de9813274bf233",
						ExpirationTime:          "",
					},
					numRetries: 0,
					ctx:        context.Background(),
				},
			},
			args: args{
				share: share{"delta_sharing", ""},
			},
			want: []schema{
				{
					Name:  "default",
					Share: "delta_sharing",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sharingClient{
				restClient: tt.fields.RestClient,
			}
			got, err := s.ListSchemas(tt.args.share)
			if (err != nil) != tt.wantErr {
				t.Errorf("SharingClient.ListSchemas() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SharingClient.ListSchemas() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSharingClient_ListTables(t *testing.T) {
	type fields struct {
		RestClient *deltaSharingRestClient
	}
	type args struct {
		schema schema
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []table
		wantErr bool
	}{
		{
			name: "Test1",
			fields: fields{
				RestClient: &deltaSharingRestClient{
					profile: &deltaSharingProfile{
						ShareCredentialsVersion: 1,
						Endpoint:                "https://sharing.delta.io/delta-sharing/",
						BearerToken:             "faaie590d541265bcab1f2de9813274bf233",
						ExpirationTime:          "",
					},
					numRetries: 0,
					ctx:        context.Background(),
				},
			},
			args: args{
				schema: schema{"default", "delta_sharing"},
			},
			want: []table{
				{Name: "COVID_19_NYT", Share: "delta_sharing", Schema: "default"},
				{Name: "boston-housing", Share: "delta_sharing", Schema: "default"},
				{Name: "flight-asa_2008", Share: "delta_sharing", Schema: "default"},
				{Name: "lending_club", Share: "delta_sharing", Schema: "default"},
				{Name: "nyctaxi_2019", Share: "delta_sharing", Schema: "default"},
				{Name: "nyctaxi_2019_part", Share: "delta_sharing", Schema: "default"},
				{Name: "owid-covid-data", Share: "delta_sharing", Schema: "default"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sharingClient{
				restClient: tt.fields.RestClient,
			}
			got, err := s.ListTables(tt.args.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("SharingClient.ListTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SharingClient.ListTables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSharingClient_ListAllTables(t *testing.T) {
	type fields struct {
		RestClient *deltaSharingRestClient
	}
	tests := []struct {
		name    string
		fields  fields
		want    []table
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				RestClient: &deltaSharingRestClient{
					profile: &deltaSharingProfile{
						ShareCredentialsVersion: 1,
						Endpoint:                "https://sharing.delta.io/delta-sharing/",
						BearerToken:             "faaie590d541265bcab1f2de9813274bf233",
						ExpirationTime:          "",
					},
					numRetries: 0,
					ctx:        context.Background(),
				},
			},
			want: []table{
				{Name: "COVID_19_NYT", Share: "delta_sharing", Schema: "default"},
				{Name: "boston-housing", Share: "delta_sharing", Schema: "default"},
				{Name: "flight-asa_2008", Share: "delta_sharing", Schema: "default"},
				{Name: "lending_club", Share: "delta_sharing", Schema: "default"},
				{Name: "nyctaxi_2019", Share: "delta_sharing", Schema: "default"},
				{Name: "nyctaxi_2019_part", Share: "delta_sharing", Schema: "default"},
				{Name: "owid-covid-data", Share: "delta_sharing", Schema: "default"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sharingClient{
				restClient: tt.fields.RestClient,
			}
			got, err := s.ListAllTables()
			if (err != nil) != tt.wantErr {
				t.Errorf("SharingClient.ListAllTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SharingClient.ListAllTables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sharingClient_ListFilesInTable(t *testing.T) {
	type fields struct {
		restClient *deltaSharingRestClient
	}
	type args struct {
		t table
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *listFilesInTableResponse
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sharingClient{
				restClient: tt.fields.restClient,
			}
			got, err := s.ListFilesInTable(tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Errorf("sharingClient.ListFilesInTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sharingClient.ListFilesInTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sharingClient_GetTableVersion(t *testing.T) {
	type fields struct {
		restClient *deltaSharingRestClient
	}
	type args struct {
		t table
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sharingClient{
				restClient: tt.fields.restClient,
			}
			got, err := s.GetTableVersion(tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Errorf("sharingClient.GetTableVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sharingClient.GetTableVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sharingClient_GetTableMetadata(t *testing.T) {
	type _fields struct {
		restClient *deltaSharingRestClient
	}
	type args struct {
		t table
	}
	tests := []struct {
		name    string
		fields  _fields
		args    args
		want    *metadata
		wantErr bool
	}{
		// tests to be defined
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sharingClient{
				restClient: tt.fields.restClient,
			}
			got, err := s.GetTableMetadata(tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Errorf("sharingClient.GetTableMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sharingClient.GetTableMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}
