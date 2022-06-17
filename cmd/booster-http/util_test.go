package main

import (
	"testing"
)

func Test_addCommas(t *testing.T) {
	type args struct {
		count int
	}
	tests := []struct {
		args args
		want string
	}{{
		args: args{count: 1},
		want: "1",
	}, {
		args: args{count: 12},
		want: "12",
	}, {
		args: args{count: 123},
		want: "123",
	}, {
		args: args{count: 1234},
		want: "1,234",
	}, {
		args: args{count: 12345},
		want: "12,345",
	}, {
		args: args{count: 123456},
		want: "123,456",
	}, {
		args: args{count: 1234567},
		want: "1,234,567",
	}, {
		args: args{count: 12345678},
		want: "12,345,678",
	}, {
		args: args{count: 123456789},
		want: "123,456,789",
	}, {
		args: args{count: 1234567890},
		want: "1,234,567,890",
	}}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := addCommas(uint64(tt.args.count)); got != tt.want {
				t.Errorf("addCommas() = %v, want %v", got, tt.want)
			}
		})
	}
}
