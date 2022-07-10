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
	"reflect"
	"testing"
)

func TestNewDeltaSharingRestClient(t *testing.T) {
	type args struct {
		ctx        context.Context
		profile    *deltaSharingProfile
		numRetries int
	}
	tests := []struct {
		name string
		args args
		want *deltaSharingRestClient
	}{
		{
			name: "test1",
			args: args{
				ctx: context.Background(),
				profile: &deltaSharingProfile{
					ShareCredentialsVersion: 1,
					Endpoint:                "https://sharing.delta.io/delta-sharing/",
					BearerToken:             "123456789",
					ExpirationTime:          "",
				},
				numRetries: 5,
			},
			want: &deltaSharingRestClient{
				profile: &deltaSharingProfile{
					ShareCredentialsVersion: 1,
					Endpoint:                "https://sharing.delta.io/delta-sharing/",
					BearerToken:             "123456789",
					ExpirationTime:          "",
				},
				numRetries: 5,
				ctx:        context.Background(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newDeltaSharingRestClient(tt.args.ctx, tt.args.profile, tt.args.numRetries); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDeltaSharingRestClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_ReadFileReader(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		url string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *bytes.Reader
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := d.readFileReader(tt.args.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.ReadFileReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DeltaSharingRestClient.ReadFileReader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_callSharingServer(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		request string
		shr     protoShare
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []share
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				Profile: &deltaSharingProfile{
					ShareCredentialsVersion: 1,
					Endpoint:                "https://sharing.delta.io/delta-sharing/",
					BearerToken:             "123456789",
					ExpirationTime:          "",
				},
				NumRetries: 5,
				Ctx:        context.Background(),
			},
			args: args{
				request: "/shares",
			},
			want: []share{
				{Name: "delta_sharing"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}

			var shares []share
			var share protoShare

			got, err := d.callSharingServer(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.callSharingServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			err = json.Unmarshal((*got)[0], &share)

			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.callSharingServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			shares = append(shares, share.Items...)

			if !reflect.DeepEqual(shares, tt.want) {
				t.Errorf("DeltaSharingRestClient.callSharingServer() = %v, want %v", shares, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_callSharingServerWithParameters(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		request   string
		maxResult int
		pageToken string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []share
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				Profile: &deltaSharingProfile{
					ShareCredentialsVersion: 1,
					Endpoint:                "https://sharing.delta.io/delta-sharing/",
					BearerToken:             "123456789",
					ExpirationTime:          "",
				},
				NumRetries: 5,
				Ctx:        context.Background(),
			},
			args: args{
				request: "/shares",
			},
			want: []share{
				{Name: "delta_sharing"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			var shares []share
			var share protoShare

			got, err := d.callSharingServerWithParameters(tt.args.request, tt.args.maxResult, tt.args.pageToken)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.callSharingServerWithParameters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			err = json.Unmarshal((*got)[0], &share)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.callSharingServerWithParameters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			shares = append(shares, share.Items...)

			if !reflect.DeepEqual(shares, tt.want) {
				t.Errorf("DeltaSharingRestClient.callSharingServerWithParameters() = %v, want %v", shares, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_getResponseHeader(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		request string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string][]string
		wantErr bool
	}{
		{
			name:    "test1",
			fields:  fields{Profile: &deltaSharingProfile{ShareCredentialsVersion: 1, Endpoint: "https://sharing.delta.io/delta-sharing/", BearerToken: "123456789", ExpirationTime: ""}, NumRetries: 5, Ctx: context.Background()},
			args:    args{request: "/shares"},
			want:    map[string][]string{"Date": {""}, "Content-Type": {"text/plain; charset=utf-8"}, "Content-Length": {"22"}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := d.getResponseHeader(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.getResponseHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got["Content-Type"], tt.want["Content-Type"]) {
				t.Errorf("DeltaSharingRestClient.getResponseHeader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_ListShares(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		maxResult int
		pageToken string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []share
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				Profile: &deltaSharingProfile{
					ShareCredentialsVersion: 1,
					Endpoint:                "https://sharing.delta.io/delta-sharing/",
					BearerToken:             "123456789",
					ExpirationTime:          "",
				},
				NumRetries: 5,
				Ctx:        context.Background(),
			},
			args: args{
				maxResult: 0,
				pageToken: "123456789",
			},
			want: []share{
				{Name: "delta_sharing"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := c.ListShares(tt.args.maxResult, tt.args.pageToken)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.ListShares() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.Shares, tt.want) {
				t.Errorf("DeltaSharingRestClient.ListShares() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_ListSchemas(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		share     share
		maxResult int
		pageToken string
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
				Profile: &deltaSharingProfile{
					ShareCredentialsVersion: 1,
					Endpoint:                "https://sharing.delta.io/delta-sharing/",
					BearerToken:             "123456789",
					ExpirationTime:          "",
				},
				NumRetries: 5,
				Ctx:        context.Background(),
			},
			args: args{
				share: share{
					Name: "delta_sharing",
				},
				maxResult: 0,
				pageToken: "123456789",
			},
			want: []schema{
				{
					Name:  "default",
					Share: "delta_sharing",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := c.ListSchemas(tt.args.share, tt.args.maxResult, tt.args.pageToken)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.ListSchemas() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.Schemas, tt.want) {
				t.Errorf("DeltaSharingRestClient.ListSchemas() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_ListTables(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		schema    schema
		maxResult int
		pageToken string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []Table
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				Profile: &deltaSharingProfile{
					ShareCredentialsVersion: 1,
					Endpoint:                "https://sharing.delta.io/delta-sharing/",
					BearerToken:             "123456789",
					ExpirationTime:          "",
				},
				NumRetries: 5,
				Ctx:        context.Background(),
			},
			args: args{
				schema: schema{
					Name:  "default",
					Share: "delta_sharing",
				},
				maxResult: 0,
				pageToken: "123456789",
			},
			want: []Table{
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
			c := deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := c.ListTables(tt.args.schema, tt.args.maxResult, tt.args.pageToken)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.ListTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.Tables, tt.want) {
				t.Errorf("DeltaSharingRestClient.ListTables() = %v, want %v", got.Tables, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_ListAllTables(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		share     share
		maxResult int
		pageToken string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []Table
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				Profile: &deltaSharingProfile{
					ShareCredentialsVersion: 1,
					Endpoint:                "https://sharing.delta.io/delta-sharing/",
					BearerToken:             "123456789",
					ExpirationTime:          "",
				},
				NumRetries: 5,
				Ctx:        context.Background(),
			},
			args: args{
				share: share{
					Name: "delta_sharing",
				},
				maxResult: 0,
				pageToken: "123456789",
			},
			want: []Table{
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
			c := deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := c.ListAllTables(tt.args.share, tt.args.maxResult, tt.args.pageToken)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.ListAllTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.Tables, tt.want) {
				t.Errorf("DeltaSharingRestClient.ListAllTables() = %v, want %v", got.Tables, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_QueryTableVersion(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		table Table
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			name:    "test1",
			fields:  fields{Profile: &deltaSharingProfile{ShareCredentialsVersion: 1, Endpoint: "https://sharing.delta.io/delta-sharing/", BearerToken: "123456789", ExpirationTime: ""}, NumRetries: 5, Ctx: context.Background()},
			args:    args{table: Table{Name: "COVID_19_NYT", Share: "delta_sharing", Schema: "default"}},
			want:    0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := c.QueryTableVersion(tt.args.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.QueryTableVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.DeltaTableVersion, tt.want) {
				t.Errorf("DeltaSharingRestClient.QueryTableVersion() = %v, want %v", got.DeltaTableVersion, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_ListFilesInTable(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		table Table
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    float32
		wantErr bool
	}{
		{
			name:    "test1",
			fields:  fields{Profile: &deltaSharingProfile{ShareCredentialsVersion: 1, Endpoint: "https://sharing.delta.io/delta-sharing/", BearerToken: "123456789", ExpirationTime: ""}, NumRetries: 5, Ctx: context.Background()},
			args:    args{table: Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"}},
			want:    27348,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := c.ListFilesInTable(tt.args.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.ListFilesInTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.AddFiles[0].Size, tt.want) {
				t.Errorf("DeltaSharingRestClient.ListFilesInTable() = %v, want %v", got.AddFiles[0].Size, tt.want)
			}
		})
	}
}

func TestDeltaSharingRestClient_postQuery(t *testing.T) {
	type fields struct {
		Profile    *deltaSharingProfile
		NumRetries int
		Ctx        context.Context
	}
	type args struct {
		request        string
		predicateHints []string
		limitHint      int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *[][]byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &deltaSharingRestClient{
				profile:    tt.fields.Profile,
				numRetries: tt.fields.NumRetries,
				ctx:        tt.fields.Ctx,
			}
			got, err := c.postQuery(tt.args.request, tt.args.predicateHints, tt.args.limitHint)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeltaSharingRestClient.postQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DeltaSharingRestClient.postQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
