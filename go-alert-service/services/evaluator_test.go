package services

import "testing"

func TestThresholdCrossed(t *testing.T) {
	tests := []struct {
		name      string
		value     float64
		operator  string
		threshold float64
		want      bool
	}{
		// gt
		{"gt: value above threshold", 85.0, "gt", 80.0, true},
		{"gt: value below threshold", 75.0, "gt", 80.0, false},
		{"gt: value equal to threshold", 80.0, "gt", 80.0, false},
		// lt
		{"lt: value below threshold", 75.0, "lt", 80.0, true},
		{"lt: value above threshold", 85.0, "lt", 80.0, false},
		{"lt: value equal to threshold", 80.0, "lt", 80.0, false},
		// gte
		{"gte: value above threshold", 85.0, "gte", 80.0, true},
		{"gte: value equal to threshold", 80.0, "gte", 80.0, true},
		{"gte: value below threshold", 75.0, "gte", 80.0, false},
		// lte
		{"lte: value below threshold", 75.0, "lte", 80.0, true},
		{"lte: value equal to threshold", 80.0, "lte", 80.0, true},
		{"lte: value above threshold", 85.0, "lte", 80.0, false},
		// eq
		{"eq: value matches threshold", 80.0, "eq", 80.0, true},
		{"eq: value does not match threshold", 80.1, "eq", 80.0, false},
		// unknown operator
		{"unknown operator returns false", 80.0, "ne", 80.0, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := thresholdCrossed(tc.value, tc.operator, tc.threshold)
			if got != tc.want {
				t.Errorf("thresholdCrossed(%v, %q, %v) = %v, want %v",
					tc.value, tc.operator, tc.threshold, got, tc.want)
			}
		})
	}
}
