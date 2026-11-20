package cli

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestStatsText(t *testing.T) {
	d := browsing(rejectedOrder, orphan, expiredPayment, rejectedOrder)

	code, out, errOut := runWith(t, fakeBrokers(d), "stats", "orders.dlq", "--url", "fake://x")
	if code != exitOK {
		t.Fatalf("exit code = %d, stderr:\n%s", code, errOut)
	}

	for _, want := range []string{
		"orders.dlq  stats",
		"Messages",
		expiredPayment.Deaths[0].Time.Local().Format(time.DateTime),
		rejectedAt.Local().Format(time.DateTime),
		"rejected ×2",
		"expired ×1",
		"none ×1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Matched") {
		t.Errorf("stdout shows a match count without filters:\n%s", out)
	}
	for _, want := range []string{"up to 18 times", "Put 4 messages back on orders.dlq"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr is missing %q:\n%s", want, errOut)
		}
	}
}

func TestStatsJSON(t *testing.T) {
	d := browsing(rejectedOrder, orphan, expiredPayment, rejectedOrder)

	code, out, errOut := runWith(t, fakeBrokers(d), "stats", "orders.dlq", "--url", "fake://x", "--not-reason", "expired", "-o", "json")
	want := `{"queue":"orders.dlq","depth":4,"scanned":4,"matched":3,"oldest":"2026-11-18T14:05:40Z",` +
		`"newest":"2026-11-18T14:05:40Z","reasons":{"none":1,"rejected":2}}` + "\n"
	if code != exitOK || out != want {
		t.Errorf("exit code %d, stdout =\n%s\nwant exit 0 and\n%s\nstderr:\n%s", code, out, want, errOut)
	}
}

func TestStatsInterrupted(t *testing.T) {
	d := browsing(rejectedOrder, expiredPayment)
	d.depths["orders.dlq"], d.err = 4, context.Canceled

	code, out, errOut := runWith(t, fakeBrokers(d), "stats", "orders.dlq", "--url", "fake://x")
	if code != exitCanceled || !strings.Contains(out, "2 of 4") || !strings.Contains(errOut, "these counts are partial") {
		t.Errorf("exit code %d, stdout:\n%s\nstderr:\n%s\nwant exit %d with partial counts", code, out, errOut, exitCanceled)
	}
}
