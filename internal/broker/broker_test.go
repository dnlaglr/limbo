package broker

import (
	"context"
	"net/url"
	"strings"
	"testing"
)

func TestRegistryOpen(t *testing.T) {
	var opened string
	opener := func(name string) Opener {
		return func(context.Context, *url.URL) (Driver, error) {
			opened = name
			return nil, nil
		}
	}
	r := Registry{"amqp": opener("amqp"), "amqps": opener("amqps")}

	tests := []struct {
		raw     string
		want    string
		wantErr string
	}{
		{raw: "amqp://guest:guest@localhost:5672/", want: "amqp"},
		{raw: "AMQPS://broker:5671/prod", want: "amqps"},
		{raw: "redis://localhost:6379", wantErr: `unsupported broker URL scheme "redis" (use amqp://, amqps://)`},
		{raw: "localhost:5672", wantErr: `unsupported broker URL scheme "localhost"`},
		{raw: "amqp://user:s3cret@[::1", wantErr: "invalid broker URL"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			opened = ""
			u, err := ParseURL(tt.raw)
			if err == nil {
				_, err = r.Open(context.Background(), u)
			}

			if tt.wantErr == "" {
				if err != nil || opened != tt.want {
					t.Fatalf("opened %q, err %v; want %q", opened, err, tt.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Errorf("err leaks the password: %v", err)
			}
		})
	}
}
