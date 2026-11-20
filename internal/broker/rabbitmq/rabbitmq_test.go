package rabbitmq

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dnlaglr/limbo/internal/broker"
)

func TestToMessage(t *testing.T) {
	rejectedAt := time.Date(2026, 11, 18, 14, 5, 40, 0, time.UTC)
	expiredAt := rejectedAt.Add(-time.Minute)

	got := toMessage("orders.dlq", &amqp.Delivery{
		MessageId:   "4f1c2a7e",
		ContentType: "application/json",
		Body:        []byte(`{"orderId":1042}`),
		Headers: amqp.Table{
			"tenant":              "acme",
			"x-delivery-count":    int64(4),
			"x-first-death-queue": "orders.retry",
			"x-last-death-reason": "rejected",
			"x-death": []any{
				amqp.Table{"queue": "orders", "reason": "rejected", "count": int64(3), "time": rejectedAt, "exchange": "shop", "routing-keys": []any{"order.created"}},
				amqp.Table{"queue": "orders.retry", "reason": "expired", "count": int64(1), "time": expiredAt, "exchange": "", "routing-keys": []any{"orders"}},
			},
		},
	})

	want := broker.Message{
		ID:            "4f1c2a7e",
		Queue:         "orders.dlq",
		Body:          []byte(`{"orderId":1042}`),
		ContentType:   "application/json",
		DeliveryCount: 4,
		Headers:       map[string]any{"tenant": "acme"},
		Deaths: []broker.Death{
			{Queue: "orders", Reason: "rejected", Count: 3, Time: rejectedAt, Exchange: "shop", RoutingKeys: []string{"order.created"}},
			{Queue: "orders.retry", Reason: "expired", Count: 1, Time: expiredAt, RoutingKeys: []string{"orders"}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("toMessage() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestToMessageWithoutHeaders(t *testing.T) {
	got := toMessage("q", &amqp.Delivery{Body: []byte("x")})
	if got.Headers != nil || len(got.Deaths) != 0 || got.DeliveryCount != 0 {
		t.Errorf("toMessage() = %+v, want no headers, deaths or deliveries", got)
	}
}

func TestRepublishing(t *testing.T) {
	now := time.Date(2026, 11, 19, 9, 30, 0, 0, time.UTC)

	got := republishing(&amqp.Delivery{
		MessageId:     "4f1c2a7e",
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		Priority:      5,
		CorrelationId: "c-1",
		Timestamp:     now.Add(-time.Hour),
		Body:          []byte(`{"orderId":1042}`),
		Headers: amqp.Table{
			"tenant":              "acme",
			"x-delivery-count":    int64(4),
			"x-acquired-count":    int64(6),
			"x-first-death-queue": "orders",
			"x-last-death-reason": "rejected",
			"x-death":             []any{amqp.Table{"queue": "orders", "reason": "rejected", "count": int64(3)}},
			replayCountHeader:     int64(2),
		},
	}, now)

	want := amqp.Publishing{
		MessageId:     "4f1c2a7e",
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		Priority:      5,
		CorrelationId: "c-1",
		Timestamp:     now.Add(-time.Hour),
		Body:          []byte(`{"orderId":1042}`),
		Headers:       amqp.Table{"tenant": "acme", replayCountHeader: int64(3), replayedAtHeader: now},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("republishing() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestPeekLeavesMessages(t *testing.T) {
	for _, queueType := range []string{"classic", "quorum"} {
		t.Run(queueType, func(t *testing.T) {
			ctx := context.Background()
			d, ch := connect(t)
			source := testQueue(queueType)
			dlq := deadLetter(t, ch, source, queueType, 3)

			for range 3 {
				msgs, err := d.Peek(ctx, dlq, 2)
				if err != nil {
					t.Fatal(err)
				}
				if len(msgs) != 2 || msgs[0].ID != "m0" || msgs[1].ID != "m1" {
					t.Fatalf("peeked %+v, want m0 and m1 in order", msgs)
				}
				if deaths := msgs[0].Deaths; len(deaths) != 1 || deaths[0].Queue != source || deaths[0].Reason != "rejected" {
					t.Fatalf("deaths = %+v, want one rejection from %s", deaths, source)
				}
				waitForDepth(t, ch, dlq, 3)
			}

			missing := source + ".missing"
			if _, err := d.Peek(ctx, missing, 1); err == nil || err.Error() != `queue "`+missing+`" not found` {
				t.Errorf("peek of a missing queue: err = %v", err)
			}
		})
	}
}

func TestReplay(t *testing.T) {
	for _, queueType := range []string{"classic", "quorum"} {
		t.Run(queueType, func(t *testing.T) {
			ctx := context.Background()
			d, ch := connect(t)
			source := testQueue(queueType)
			dlq := deadLetter(t, ch, source, queueType, 5)
			publish(t, ch, dlq, amqp.Publishing{MessageId: "orphan"})
			waitForDepth(t, ch, dlq, 6)

			if n, err := d.Depth(ctx, dlq); n != 6 || err != nil {
				t.Fatalf("Depth() = %d, %v; want 6", n, err)
			}

			r, ok := d.(broker.Replayer)
			if !ok {
				t.Fatal("the RabbitMQ driver does not implement broker.Replayer")
			}
			replay := func(ctx context.Context, opts broker.ReplayOptions, wantErr error, want ...string) {
				t.Helper()
				var got []string
				err := r.Replay(ctx, dlq, opts, func(r broker.Replayed) {
					got = append(got, outcome(r))
				})
				if !errors.Is(err, wantErr) || !slices.Equal(got, want) {
					t.Fatalf("Replay(%+v) = %q, %v; want %q, %v", opts, got, err, want, wantErr)
				}
			}

			replay(ctx, broker.ReplayOptions{Limit: 2}, nil, "m0 → "+source, "m1 → "+source)
			waitForDepth(t, ch, dlq, 4)

			missing := source + ".missing"
			unroutable := ` ✗ no queue named "` + missing + `"`
			replay(ctx, broker.ReplayOptions{To: missing, Limit: 2}, nil, "m2 → "+missing+unroutable, "m3 → "+missing+unroutable)
			waitForDepth(t, ch, dlq, 4)
			if ids := peekIDs(t, d, dlq, 4); !slices.Equal(ids, []string{"m2", "m3", "m4", "orphan"}) {
				t.Fatalf("%s holds %q after unroutable replays, want the original order", dlq, ids)
			}

			canceled, cancel := context.WithCancel(ctx)
			err := r.Replay(canceled, dlq, broker.ReplayOptions{Limit: 4}, func(broker.Replayed) { cancel() })
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled Replay() err = %v, want context.Canceled", err)
			}
			waitForDepth(t, ch, dlq, 3)

			replay(ctx, broker.ReplayOptions{Limit: 3}, nil, "m3 → "+source, "m4 → "+source, "orphan →  ✗ "+broker.ErrNoOrigin.Error())
			waitForDepth(t, ch, dlq, 1)
			waitForDepth(t, ch, source, 5)

			replayed, err := d.Peek(ctx, source, 5)
			if err != nil {
				t.Fatal(err)
			}
			for i, m := range replayed {
				if m.ID != "m"+strconv.Itoa(i) || len(m.Deaths) != 0 || len(m.Headers) != 2 || m.Headers[replayCountHeader] != int64(1) {
					t.Errorf("replayed message %d = %+v, want m%d with only limbo's headers and replayed once", i, m, i)
				}
			}
		})
	}
}

func outcome(r broker.Replayed) string {
	s := r.Message.ID + " → " + r.To
	if r.Err != nil {
		s += " ✗ " + r.Err.Error()
	}

	return s
}

func peekIDs(t *testing.T, d broker.Driver, queue string, n int) []string {
	t.Helper()
	msgs, err := d.Peek(context.Background(), queue, n)
	if err != nil {
		t.Fatal(err)
	}

	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}

	return ids
}

// Requires a broker, e.g. LIMBO_TEST_AMQP_URL=amqp://guest:guest@localhost:5672/
func connect(t *testing.T) (broker.Driver, *amqp.Channel) {
	t.Helper()
	rawURL := os.Getenv("LIMBO_TEST_AMQP_URL")
	if rawURL == "" {
		t.Skip("LIMBO_TEST_AMQP_URL is not set")
	}
	u, err := broker.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}

	d, err := Open(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })

	conn, err := amqp.Dial(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	return d, ch
}

func testQueue(queueType string) string {
	return "limbo.test." + queueType + "." + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// Rejects n messages from source so they land in its DLQ, in order and with an x-death header
func deadLetter(t *testing.T, ch *amqp.Channel, source, queueType string, n int) string {
	t.Helper()
	dlq := source + ".dlq"
	declare := func(name string, args amqp.Table) {
		args["x-queue-type"] = queueType
		if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = ch.QueueDelete(name, false, false, false) })
	}
	declare(dlq, amqp.Table{})
	declare(source, amqp.Table{"x-dead-letter-exchange": "", "x-dead-letter-routing-key": dlq})

	for i := range n {
		publish(t, ch, source, amqp.Publishing{MessageId: "m" + strconv.Itoa(i), Body: []byte(`{"n":` + strconv.Itoa(i) + `}`)})
	}
	waitForDepth(t, ch, source, n)

	for range n {
		d, ok, err := ch.Get(source, false)
		if err != nil || !ok {
			t.Fatalf("get from %s: ok %v, err %v", source, ok, err)
		}
		if err := d.Reject(false); err != nil {
			t.Fatal(err)
		}
	}
	waitForDepth(t, ch, dlq, n)

	return dlq
}

func publish(t *testing.T, ch *amqp.Channel, queue string, msg amqp.Publishing) {
	t.Helper()
	if err := ch.PublishWithContext(context.Background(), "", queue, false, false, msg); err != nil {
		t.Fatal(err)
	}
}

// Quorum queues apply returns asynchronously, so depth settles shortly after a peek
func waitForDepth(t *testing.T, ch *amqp.Channel, queue string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for got := depth(t, ch, queue); got != n; got = depth(t, ch, queue) {
		if time.Now().After(deadline) {
			t.Fatalf("%s holds %d messages, want %d", queue, got, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func depth(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	return q.Messages
}
