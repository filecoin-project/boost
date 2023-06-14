package main

import "testing"

func TestGetConnStringWithDb(t *testing.T) {
	type args struct {
		connString string
		dbName     string
	}
	tests := []struct {
		name   string
		args   args
		expect string
	}{{
		name: "connect string with no db or args",
		args: args{
			connString: "postgresql://postgres:postgres@localhost",
			dbName:     "bench",
		},
		expect: "postgresql://postgres:postgres@localhost/bench",
	}, {
		name: "connect string with args but no db",
		args: args{
			connString: "postgresql://postgres:postgres@localhost?sslmode=disable",
			dbName:     "bench",
		},
		expect: "postgresql://postgres:postgres@localhost/bench?sslmode=disable",
	}, {
		name: "connect string with db but no args",
		args: args{
			connString: "postgresql://postgres:postgres@localhost/somedb",
			dbName:     "bench",
		},
		expect: "postgresql://postgres:postgres@localhost/bench",
	}, {
		name: "connect string with db and args",
		args: args{
			connString: "postgresql://postgres:postgres@localhost/somedb?sslmode=disable",
			dbName:     "bench",
		},
		expect: "postgresql://postgres:postgres@localhost/bench?sslmode=disable",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getConnStringWithDb(tt.args.connString, tt.args.dbName)
			if err != nil {
				t.Error(err)
			}
			if got != tt.expect {
				t.Errorf("getConnStringWithDb(%v, %v)\n  returns  %v\n  expected %v", tt.args.connString, tt.args.dbName, got, tt.expect)
			}
		})
	}
}
