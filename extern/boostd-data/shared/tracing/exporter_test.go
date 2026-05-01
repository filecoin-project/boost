package tracing

import "testing"

func TestNormalizeTracingEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{
			name:     "legacy jaeger default",
			endpoint: "http://tempo:14268/api/traces",
			want:     "http://tempo:4318/v1/traces",
		},
		{
			name:     "legacy jaeger path without default port",
			endpoint: "https://tempo.example.com/api/traces",
			want:     "https://tempo.example.com/v1/traces",
		},
		{
			name:     "otlp endpoint unchanged",
			endpoint: "http://tempo:4318/v1/traces",
			want:     "http://tempo:4318/v1/traces",
		},
		{
			name:     "non url unchanged",
			endpoint: "tempo:4318",
			want:     "tempo:4318",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := normalizeTracingEndpoint(tc.endpoint)
			if got != tc.want {
				t.Fatalf("normalizeTracingEndpoint(%q) = %q, want %q", tc.endpoint, got, tc.want)
			}
		})
	}
}
