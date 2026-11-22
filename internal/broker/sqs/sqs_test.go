package sqs

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dnlaglr/limbo/internal/broker"
)

const missingQueue = "limbo-test-missing"

func TestOpenRejects(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	for k, v := range map[string]string{"AWS_REGION": "", "AWS_DEFAULT_REGION": "", "AWS_PROFILE": "", "AWS_CONFIG_FILE": missing, "AWS_SHARED_CREDENTIALS_FILE": missing} {
		t.Setenv(k, v)
	}

	tests := []struct{ raw, want string }{
		{"sqs://AKIA:s3cret@us-east-1", "credentials come from the AWS environment, a profile or SSO, not the URL"},
		{"sqs://localhost:9324", "takes no port, pass ?endpoint="},
		{"sqs://us-east-1/queues/orders", "use sqs://<region>"},
		{"sqs://us-east-1?endpont=http://x", `unknown SQS URL option "endpont"`},
		{"sqs://us-east-1?endpoint=http://AKIA:s3cret@localhost:9324", "without credentials"},
		{"sqs://us-east-1?endpoint=localhost:9324", "endpoint must be a URL such as http://localhost:9324"},
		{"sqs://", "no AWS region: use sqs://<region> or set AWS_REGION"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			u, err := broker.ParseURL(tt.raw)
			if err != nil {
				t.Fatal(err)
			}

			_, err = Open(context.Background(), u)
			if err == nil || !strings.Contains(err.Error(), tt.want) || strings.Contains(err.Error(), "s3cret") {
				t.Errorf("err = %v, want it to contain %q and no secret", err, tt.want)
			}
		})
	}
}

func TestToMessage(t *testing.T) {
	tests := []struct {
		name string
		in   types.Message
		want broker.Message
	}{
		{
			name: "full",
			in: types.Message{
				MessageId: aws.String("id-1"),
				Body:      aws.String(`{"orderId":1042}`),
				Attributes: map[string]string{
					"SentTimestamp":            "1795010531000",
					"ApproximateReceiveCount":  "4",
					"DeadLetterQueueSourceArn": "arn:aws:sqs:us-east-1:123456789012:orders.fifo",
					"MessageGroupId":           "acme",
				},
				MessageAttributes: map[string]types.MessageAttributeValue{
					"tenant":  {DataType: aws.String("String"), StringValue: aws.String("acme")},
					"attempt": {DataType: aws.String("Number"), StringValue: aws.String("3")},
					"sig":     {DataType: aws.String("Binary"), BinaryValue: []byte{1, 2}},
				},
			},
			want: broker.Message{
				ID:            "id-1",
				Queue:         "orders-dlq.fifo",
				Body:          []byte(`{"orderId":1042}`),
				Timestamp:     time.UnixMilli(1795010531000).UTC(),
				DeliveryCount: 3,
				Headers:       map[string]any{"tenant": "acme", "attempt": "3", "sig": []byte{1, 2}, "MessageGroupId": "acme"},
				Deaths:        []broker.Death{{Queue: "orders.fifo", Reason: "delivery_limit", Count: 1}},
			},
		},
		{
			name: "bare",
			in:   types.Message{MessageId: aws.String("id-2"), Body: aws.String("x")},
			want: broker.Message{ID: "id-2", Queue: "orders-dlq.fifo", Body: []byte("x")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toMessage("orders-dlq.fifo", tt.in); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toMessage() =\n%+v\nwant\n%+v", got, tt.want)
			}
		})
	}
}

func TestBrowseLeavesMessages(t *testing.T) {
	for _, kind := range []string{"standard", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			ctx, fifo := context.Background(), kind == "fifo"
			d := connect(t)
			source, dlq := deadLetter(t, d, fifo, 3)

			for _, stop := range []int{2, 1, 2} {
				var msgs []broker.Message
				err := d.Browse(ctx, dlq, 3, func(m broker.Message) bool {
					msgs = append(msgs, m)
					return len(msgs) < stop
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(msgs) != stop || fifo && string(msgs[stop-1].Body) != "m"+strconv.Itoa(stop-1) {
					t.Fatalf("browsed %q, want %d messages (m0 to m%d on a FIFO queue)", bodies(msgs), stop, stop-1)
				}
				if m := msgs[0]; m.Origin() != source || m.Reason() != deathReason {
					t.Fatalf("origin %q, reason %q; want %s and %s", m.Origin(), m.Reason(), source, deathReason)
				}
				waitForDepth(t, d, dlq, 3)
			}
			if got := inOrder(fifo, browseBodies(t, d, dlq, 3)); !slices.Equal(got, []string{"m0", "m1", "m2"}) {
				t.Fatalf("%s holds %q after browses that stopped early, want m0, m1 and m2", dlq, got)
			}

			if err := d.Browse(ctx, missingQueue, 1, func(broker.Message) bool { return true }); err == nil || err.Error() != `queue "`+missingQueue+`" not found` {
				t.Errorf("browse of a missing queue: err = %v", err)
			}
		})
	}
}

func TestFIFOGroupLimit(t *testing.T) {
	d := connect(t)
	_, dlq := deadLetter(t, d, true, 11)

	visited := 0
	err := d.Browse(context.Background(), dlq, 11, func(broker.Message) bool { visited++; return true })
	if err != nil || visited != 10 {
		t.Fatalf("Browse() visited %d, err %v; want 10 of one group's 11", visited, err)
	}
	waitForDepth(t, d, dlq, 11)
}

func TestSendInput(t *testing.T) {
	now := time.Date(2026, 11, 20, 9, 30, 0, 0, time.UTC)
	attr := func(dataType, v string) types.MessageAttributeValue {
		return types.MessageAttributeValue{DataType: aws.String(dataType), StringValue: aws.String(v)}
	}
	userAttrs := func(n int) map[string]types.MessageAttributeValue {
		attrs := map[string]types.MessageAttributeValue{}
		for i := range n {
			attrs["a"+strconv.Itoa(i)] = attr("String", "v")
		}
		return attrs
	}
	marked := func(attrs map[string]types.MessageAttributeValue, count string) map[string]types.MessageAttributeValue {
		attrs = maps.Clone(attrs)
		attrs[replayCountAttr] = attr("Number", count)
		attrs[replayedAtAttr] = attr("String", "2026-11-20T09:30:00Z")
		return attrs
	}
	message := func(attrs map[string]types.MessageAttributeValue) types.Message {
		return types.Message{MessageId: aws.String("id-1"), Body: aws.String("b"), MessageAttributes: attrs}
	}
	input := func(attrs map[string]types.MessageAttributeValue) *awssqs.SendMessageInput {
		return &awssqs.SendMessageInput{QueueUrl: aws.String("url"), MessageBody: aws.String("b"), MessageAttributes: attrs}
	}

	replayedFIFO := message(map[string]types.MessageAttributeValue{
		"tenant":        attr("String", "acme"),
		replayCountAttr: attr("Number", "2"),
		replayedAtAttr:  attr("String", "2026-01-01T00:00:00Z"),
	})
	replayedFIFO.Attributes = map[string]string{"MessageGroupId": "acme"}
	wantFIFO := input(marked(map[string]types.MessageAttributeValue{"tenant": attr("String", "acme")}, "3"))
	wantFIFO.MessageGroupId, wantFIFO.MessageDeduplicationId = aws.String("acme"), aws.String("limbo-id-1")

	tests := []struct {
		name string
		to   string
		in   types.Message
		want *awssqs.SendMessageInput
	}{
		{"fifo", "orders.fifo", replayedFIFO, wantFIFO},
		{"standard", "orders", message(nil), input(marked(userAttrs(0), "1"))},
		{"eight attributes", "orders", message(userAttrs(8)), input(marked(userAttrs(8), "1"))},
		{"nine attributes", "orders", message(userAttrs(9)), input(userAttrs(9))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sendInput("url", tt.to, tt.in, now); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sendInput() =\n%+v\nwant\n%+v", got, tt.want)
			}
		})
	}
}

func TestReplay(t *testing.T) {
	for _, kind := range []string{"standard", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			fifo := kind == "fifo"
			d := connect(t)
			source, dlq := deadLetter(t, d, fifo, 4)
			send(t, d, dlq, "orphan")
			waitForDepth(t, d, dlq, 5)

			unroutable := " → " + missingQueue + ` ✗ queue "` + missingQueue + `" not found`
			replay(t, d, fifo, dlq, broker.ReplayOptions{Selection: broker.Selection{Scan: 5}, To: missingQueue},
				"m0"+unroutable, "m1"+unroutable, "m2"+unroutable, "m3"+unroutable, "orphan"+unroutable)
			waitForDepth(t, d, dlq, 5)

			canceled, cancel := context.WithCancel(context.Background())
			err := replayer(d).Replay(canceled, dlq, firstN(5), func(r broker.Replayed) {
				if r.Err == nil {
					cancel()
				}
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled Replay() err = %v, want context.Canceled", err)
			}
			waitForDepth(t, d, dlq, 4)

			var want []string
			for _, body := range browseBodies(t, d, dlq, 4) {
				if body == "orphan" {
					want = append(want, "orphan →  ✗ "+broker.ErrSkipped.Error())
				} else {
					want = append(want, body+" → "+source)
				}
			}
			fromSource := broker.Filter{Sources: broker.Rule[string]{Any: []string{source}}}
			replay(t, d, fifo, dlq, broker.ReplayOptions{Selection: broker.Selection{Filter: fromSource, Scan: 4}}, want...)
			waitForDepth(t, d, dlq, 1)

			replay(t, d, fifo, dlq, firstN(1), "orphan →  ✗ "+broker.ErrNoOrigin.Error())
			waitForDepth(t, d, dlq, 1)

			var replayed []broker.Message
			if err := d.Browse(context.Background(), source, 4, func(m broker.Message) bool { replayed = append(replayed, m); return true }); err != nil {
				t.Fatal(err)
			}
			if got := inOrder(fifo, bodies(replayed)); !slices.Equal(got, []string{"m0", "m1", "m2", "m3"}) {
				t.Fatalf("%s holds %q after the replays, want m0 to m3", source, got)
			}
			for _, m := range replayed {
				if m.Headers[replayCountAttr] != "1" || m.Headers[replayedAtAttr] == nil || len(m.Deaths) != 0 || fifo && m.Headers[groupHeader] != "g" {
					t.Errorf("replayed %s has headers %v and deaths %v, want limbo's markers, no deaths and on FIFO group g", m.Body, m.Headers, m.Deaths)
				}
			}
		})
	}
}

func TestReplayHoldsCopiesThatFailAgain(t *testing.T) {
	for _, kind := range []string{"standard", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			d := connect(t)
			source, dlq := deadLetter(t, d, kind == "fifo", 1)
			sourceURL, err := d.queueURL(ctx, source)
			if err != nil {
				t.Fatal(err)
			}

			var got []string
			err = replayer(d).Replay(ctx, dlq, firstN(2), func(r broker.Replayed) {
				got = append(got, string(r.Message.Body)+" → "+r.To)
				for range 2 {
					if _, err := d.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: &sourceURL}); err != nil {
						t.Error(err)
					}
				}
			})
			if want := []string{"m0 → " + source}; err != nil || !slices.Equal(got, want) {
				t.Fatalf("Replay() = %q, %v; want %q, with the copy that failed back into %s left alone", got, err, want, dlq)
			}
			waitForDepth(t, d, dlq, 1)
		})
	}
}

func TestReleaseSkipsLostHolds(t *testing.T) {
	ctx := context.Background()
	d := connect(t)
	_, dlq := deadLetter(t, d, false, 2)
	addr, err := d.queueURL(ctx, dlq)
	if err != nil {
		t.Fatal(err)
	}

	out, err := d.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: &addr, MaxNumberOfMessages: 2, VisibilityTimeout: 1})
	if err != nil || len(out.Messages) != 2 {
		t.Fatalf("received %d messages, err %v; want 2", len(out.Messages), err)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := d.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: &addr, MaxNumberOfMessages: 1, VisibilityTimeout: 60}); err != nil {
		t.Fatal(err)
	}

	held := map[string]string{}
	for _, m := range out.Messages {
		held[aws.ToString(m.MessageId)] = aws.ToString(m.ReceiptHandle)
	}
	if err := d.release(ctx, addr, held); err != nil {
		t.Errorf("release() of holds that ran out = %v, want nil since limbo no longer holds those messages", err)
	}
}

func firstN(n int) broker.ReplayOptions {
	return broker.ReplayOptions{Selection: broker.Selection{Scan: n}}
}

func replayer(d *driver) broker.Replayer {
	return broker.Driver(d).(broker.Replayer)
}

func replay(t *testing.T, d *driver, fifo bool, queue string, opts broker.ReplayOptions, want ...string) {
	t.Helper()
	var got []string
	err := replayer(d).Replay(context.Background(), queue, opts, func(r broker.Replayed) {
		s := string(r.Message.Body) + " → " + r.To
		if r.Err != nil {
			s += " ✗ " + r.Err.Error()
		}
		got = append(got, s)
	})
	if err != nil || !slices.Equal(inOrder(fifo, got), inOrder(fifo, want)) {
		t.Fatalf("Replay(%+v) = %q, %v; want %q", opts, got, err, want)
	}
}

// Requires SQS, e.g. LIMBO_TEST_SQS_URL='sqs://us-east-1?endpoint=http://localhost:9324' with dummy AWS keys for ElasticMQ
func connect(t *testing.T) *driver {
	t.Helper()
	raw := os.Getenv("LIMBO_TEST_SQS_URL")
	if raw == "" {
		t.Skip("LIMBO_TEST_SQS_URL is not set")
	}
	u, err := broker.ParseURL(raw)
	if err != nil {
		t.Fatal(err)
	}

	d, err := Open(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })

	return d.(*driver)
}

// Sends n messages to a new source queue and receives them until its redrive policy has moved them all to its DLQ
func deadLetter(t *testing.T, d *driver, fifo bool, n int) (source, dlq string) {
	t.Helper()
	ctx := context.Background()
	base := "limbo-test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	source, dlq = queueName(base, fifo), queueName(base+"-dlq", fifo)

	dlqURL := createQueue(t, d, dlq, map[string]string{})
	attrs, err := d.client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       &dlqURL,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := `{"deadLetterTargetArn":"` + attrs.Attributes["QueueArn"] + `","maxReceiveCount":"1"}`
	sourceURL := createQueue(t, d, source, map[string]string{"VisibilityTimeout": "0", "RedrivePolicy": policy})

	for i := range n {
		send(t, d, source, "m"+strconv.Itoa(i))
	}
	deadline := time.Now().Add(10 * time.Second)
	for depth(t, d, dlq) < n {
		if time.Now().After(deadline) {
			t.Fatalf("%s holds %d messages, want %d", dlq, depth(t, d, dlq), n)
		}
		if _, err := d.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: &sourceURL, MaxNumberOfMessages: maxBatch}); err != nil {
			t.Fatal(err)
		}
	}

	return source, dlq
}

func queueName(base string, fifo bool) string {
	if fifo {
		return base + ".fifo"
	}

	return base
}

func createQueue(t *testing.T, d *driver, name string, attrs map[string]string) string {
	t.Helper()
	if strings.HasSuffix(name, ".fifo") {
		attrs["FifoQueue"] = "true"
	}

	out, err := d.client.CreateQueue(context.Background(), &awssqs.CreateQueueInput{QueueName: &name, Attributes: attrs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = d.client.DeleteQueue(context.Background(), &awssqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
	})

	return aws.ToString(out.QueueUrl)
}

func send(t *testing.T, d *driver, queue, body string) {
	t.Helper()
	url, err := d.queueURL(context.Background(), queue)
	if err != nil {
		t.Fatal(err)
	}

	in := &awssqs.SendMessageInput{QueueUrl: &url, MessageBody: &body}
	if strings.HasSuffix(queue, ".fifo") {
		in.MessageGroupId, in.MessageDeduplicationId = aws.String("g"), &body
	}
	if _, err := d.client.SendMessage(context.Background(), in); err != nil {
		t.Fatal(err)
	}
}

func browseBodies(t *testing.T, d *driver, queue string, n int) []string {
	t.Helper()
	var msgs []broker.Message
	if err := d.Browse(context.Background(), queue, n, func(m broker.Message) bool { msgs = append(msgs, m); return true }); err != nil {
		t.Fatal(err)
	}

	return bodies(msgs)
}

func bodies(msgs []broker.Message) []string {
	s := make([]string, len(msgs))
	for i, m := range msgs {
		s[i] = string(m.Body)
	}

	return s
}

// Standard queues hand messages out in no fixed order, so their results compare sorted
func inOrder(fifo bool, s []string) []string {
	if !fifo {
		slices.Sort(s)
	}

	return s
}

func waitForDepth(t *testing.T, d *driver, queue string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for got := depth(t, d, queue); got != n; got = depth(t, d, queue) {
		if time.Now().After(deadline) {
			t.Fatalf("%s holds %d messages, want %d", queue, got, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func depth(t *testing.T, d *driver, queue string) int {
	t.Helper()
	n, err := d.Depth(context.Background(), queue)
	if err != nil {
		t.Fatal(err)
	}

	return n
}
