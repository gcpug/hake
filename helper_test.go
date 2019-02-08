package hake_test

import (
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

func date(t *testing.T, s string) civil.Date {
	t.Helper()
	d, err := civil.ParseDate(s)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	return d
}

func timestamp(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	return tm
}
