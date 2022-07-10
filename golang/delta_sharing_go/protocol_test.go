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
)

func TestFile_GetStats(t *testing.T) {
	type fields struct {
		restClient *deltaSharingRestClient
	}
	type args struct {
		t Table
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *stats
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				restClient: &deltaSharingRestClient{
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
				t: Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"},
			},
			want: &stats{
				NumRecords: 506,
				MinValues:  map[string]interface{}{"tax": float64(187), "rm": float64(3.561), "age": float64(2.9), "crim": float64(0.00632), "zn": float64(0), "nox": float64(0.385), "rad": float64(1), "ptratio": float64(12.6), "black": float64(0.32), "medv": float64(5), "ID": float64(1), "indus": float64(0.46), "lstat": float64(1.73), "chas": float64(0), "dis": float64(1.1296)},
				MaxValues:  map[string]interface{}{"tax": float64(711), "rm": float64(8.78), "age": float64(100), "crim": float64(88.9762), "zn": float64(100), "nox": float64(0.871), "rad": float64(24), "ptratio": float64(22), "black": float64(396.9), "medv": float64(50), "ID": float64(506), "indus": float64(27.74), "lstat": float64(37.97), "chas": float64(1), "dis": float64(12.1265)},
				NullCount:  map[string]interface{}{"tax": float64(0), "rm": float64(0), "age": float64(0), "crim": float64(0), "zn": float64(0), "nox": float64(0), "rad": float64(0), "ptratio": float64(0), "black": float64(0), "medv": float64(173), "ID": float64(0), "indus": float64(0), "lstat": float64(0), "chas": float64(0), "dis": float64(0)},
			},
			wantErr: false,
		},
		{
			name: "test2",
			fields: fields{
				restClient: &deltaSharingRestClient{
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
				t: Table{Name: "boston-hoing", Share: "delta_sharing", Schema: "default"},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := *tt.fields.restClient
			F, err := d.ListFilesInTable(tt.args.t)
			if (err != nil) == tt.wantErr {
				return
			}
			got, err := F.AddFiles[0].GetStats()
			if (err != nil) != tt.wantErr {
				t.Errorf("File.GetStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("File.GetStats() = %v, want %v", got, tt.want)
			}
		})
	}
}
