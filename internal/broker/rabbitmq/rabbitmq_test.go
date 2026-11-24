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
		MessageId:     "4f1c2a7e",
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: "c-9",
		AppId:         "shop",
		Body:          []byte(`{"orderId":1042}`),
		Headers: amqp.Table{
			"tenant":              "acme",
			"vip":                 true,
			"attempt":             int32(3),
			"ratio":               float64(0.5),
			"price":               amqp.Decimal{Scale: 2, Value: 1999},
			"sent-at":             expiredAt,
			"sig":                 []byte{1, 2},
			"trace":               amqp.Table{"hop": int8(1)},
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
		Headers: map[string]any{
			"tenant":  "acme",
			"vip":     true,
			"attempt": int32(3),
			"ratio":   float64(0.5),
			"price":   amqp.Decimal{Scale: 2, Value: 1999},
			"sent-at": expiredAt,
			"sig":     []byte{1, 2},
			"trace":   amqp.Table{"hop": int8(1)},
		},
		Properties: map[string]any{
			"delivery_mode":  uint8(2),
			"correlation_id": "c-9",
			"app_id":         "shop",
			"header_types":   map[string]string{"attempt": "int32", "ratio": "float64", "price": "decimal", "sent-at": "timestamp", "sig": "bytes"},
		},
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
	if got.Headers != nil || got.Properties != nil || len(got.Deaths) != 0 || got.DeliveryCount != 0 {
		t.Errorf("toMessage() = %+v, want no headers, properties, deaths or deliveries", got)
	}
}

func TestRepublishing(t *testing.T) {
	now := time.Date(2026, 11, 19, 9, 30, 0, 0, time.UTC)

	got := republishing(toMessage("orders.dlq", &amqp.Delivery{
		MessageId:       "4f1c2a7e",
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		DeliveryMode:    amqp.Persistent,
		Priority:        5,
		CorrelationId:   "c-1",
		ReplyTo:         "replies",
		Expiration:      "60000",
		Type:            "order.created",
		UserId:          "guest",
		AppId:           "shop",
		Timestamp:       now.Add(-time.Hour),
		Body:            []byte(`{"orderId":1042}`),
		Headers: amqp.Table{
			"tenant":              "acme",
			"x-delivery-count":    int64(4),
			"x-acquired-count":    int64(6),
			"x-first-death-queue": "orders",
			"x-last-death-reason": "rejected",
			"x-death":             []any{amqp.Table{"queue": "orders", "reason": "rejected", "count": int64(3)}},
			replayCountHeader:     int64(2),
		},
	}), now)

	want := amqp.Publishing{
		MessageId:       "4f1c2a7e",
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		DeliveryMode:    amqp.Persistent,
		Priority:        5,
		CorrelationId:   "c-1",
		ReplyTo:         "replies",
		Expiration:      "60000",
		Type:            "order.created",
		UserId:          "guest",
		AppId:           "shop",
		Timestamp:       now.Add(-time.Hour),
		Body:            []byte(`{"orderId":1042}`),
		Headers:         amqp.Table{"tenant": "acme", replayCountHeader: int64(3), replayedAtHeader: now},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("republishing() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestBrowseLeavesMessages(t *testing.T) {
	for _, queueType := range []string{"classic", "quorum"} {
		t.Run(queueType, func(t *testing.T) {
			ctx := context.Background()
			d, ch := connect(t)
			source := testQueue(queueType)
			dlq := deadLetter(t, ch, source, queueType, 3)

			for _, stop := range []int{2, 1, 2} {
				var msgs []broker.Message
				err := d.Browse(ctx, dlq, 3, func(m broker.Message) bool {
					msgs = append(msgs, m)
					return len(msgs) < stop
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(msgs) != stop || msgs[0].ID != "m0" || msgs[stop-1].ID != "m"+strconv.Itoa(stop-1) {
					t.Fatalf("browsed %+v, want m0 to m%d in order", msgs, stop-1)
				}
				if deaths := msgs[0].Deaths; len(deaths) != 1 || deaths[0].Queue != source || deaths[0].Reason != "rejected" {
					t.Fatalf("deaths = %+v, want one rejection from %s", deaths, source)
				}
				waitForDepth(t, ch, dlq, 3)
			}
			if ids := browseIDs(t, d, dlq, 3); !slices.Equal(ids, []string{"m0", "m1", "m2"}) {
				t.Fatalf("%s holds %q after browses that stopped early, want the original order", dlq, ids)
			}

			missing := source + ".missing"
			if err := d.Browse(ctx, missing, 1, func(broker.Message) bool { return true }); err == nil || err.Error() != `queue "`+missing+`" not found` {
				t.Errorf("browse of a missing queue: err = %v", err)
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

			replay(t, d, dlq, firstN(2), nil, "m0 → "+source, "m1 → "+source)
			waitForDepth(t, ch, dlq, 4)

			missing := source + ".missing"
			unroutable := ` ✗ no queue named "` + missing + `"`
			replay(t, d, dlq, broker.ReplayOptions{Selection: broker.Selection{Scan: 2}, To: missing}, nil, "m2 → "+missing+unroutable, "m3 → "+missing+unroutable)
			waitForDepth(t, ch, dlq, 4)
			if ids := browseIDs(t, d, dlq, 4); !slices.Equal(ids, []string{"m2", "m3", "m4", "orphan"}) {
				t.Fatalf("%s holds %q after unroutable replays, want the original order", dlq, ids)
			}

			canceled, cancel := context.WithCancel(ctx)
			err := broker.Replay(canceled, d.(broker.Replayer), dlq, firstN(4), func(broker.Taken) { cancel() })
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled Replay() err = %v, want context.Canceled", err)
			}
			waitForDepth(t, ch, dlq, 3)

			replay(t, d, dlq, firstN(3), nil, "m3 → "+source, "m4 → "+source, "orphan →  ✗ "+broker.ErrNoOrigin.Error())
			waitForDepth(t, ch, dlq, 1)
			waitForDepth(t, ch, source, 5)

			var replayed []broker.Message
			if err := d.Browse(ctx, source, 5, func(m broker.Message) bool { replayed = append(replayed, m); return true }); err != nil {
				t.Fatal(err)
			}
			for i, m := range replayed {
				if m.ID != "m"+strconv.Itoa(i) || len(m.Deaths) != 0 || len(m.Headers) != 2 || m.Headers[replayCountHeader] != int64(1) {
					t.Errorf("replayed message %d = %+v, want m%d with only limbo's headers and replayed once", i, m, i)
				}
				want := map[string]any{
					"delivery_mode": uint8(2),
					"app_id":        "limbo-test",
					"header_types":  map[string]string{replayCountHeader: "int64", replayedAtHeader: "timestamp"},
				}
				if !reflect.DeepEqual(m.Properties, want) {
					t.Errorf("replayed message %d has properties %v, want %v", i, m.Properties, want)
				}
			}
		})
	}
}

func TestFilteredReplay(t *testing.T) {
	for _, queueType := range []string{"classic", "quorum"} {
		t.Run(queueType, func(t *testing.T) {
			d, ch := connect(t)
			base := testQueue(queueType)
			dlq, orders, payments := base+".dlq", base+".orders", base+".payments"
			declare(t, ch, dlq, queueType, amqp.Table{})
			declare(t, ch, orders, queueType, deadLettersTo(dlq))
			capped := deadLettersTo(dlq)
			capped["x-max-length"] = int64(1)
			declare(t, ch, payments, queueType, capped)

			reject(t, ch, orders, amqp.Publishing{MessageId: "m0"})
			waitForDepth(t, ch, dlq, 1)
			reject(t, ch, payments, amqp.Publishing{MessageId: "m1"})
			waitForDepth(t, ch, dlq, 2)
			publish(t, ch, payments, amqp.Publishing{MessageId: "m2"})
			publish(t, ch, payments, amqp.Publishing{MessageId: "overflow"})
			waitForDepth(t, ch, dlq, 3)
			reject(t, ch, orders, amqp.Publishing{MessageId: "m3", Headers: amqp.Table{"tenant": "acme"}})
			waitForDepth(t, ch, dlq, 4)
			publish(t, ch, dlq, amqp.Publishing{MessageId: "orphan"})
			waitForDepth(t, ch, dlq, 5)

			var seeded []string
			err := d.Browse(context.Background(), dlq, 5, func(m broker.Message) bool {
				seeded = append(seeded, m.ID+" "+m.Reason()+" "+m.Origin())
				return true
			})
			want := []string{"m0 rejected " + orders, "m1 rejected " + payments, "m2 maxlen " + payments, "m3 rejected " + orders, "orphan  "}
			if err != nil || !slices.Equal(seeded, want) {
				t.Fatalf("seeded %q, %v; want %q", seeded, err, want)
			}
			waitForDepth(t, ch, dlq, 5)

			skipped := " ✗ " + broker.ErrSkipped.Error()
			fromOrders := broker.Filter{Sources: broker.Rule[string]{Any: []string{orders}}}
			replay(t, d, dlq, broker.ReplayOptions{Selection: broker.Selection{Filter: fromOrders, Scan: 5}}, nil,
				"m0 → "+orders, "m1 → "+skipped, "m2 → "+skipped, "m3 → "+orders, "orphan → "+skipped)
			waitForDepth(t, ch, dlq, 3)
			if ids := browseIDs(t, d, dlq, 3); !slices.Equal(ids, []string{"m1", "m2", "orphan"}) {
				t.Fatalf("%s holds %q after a filtered replay, want the skipped messages in order", dlq, ids)
			}

			overflowed := broker.Filter{Reasons: broker.Rule[string]{Any: []string{"maxlen"}}}
			replay(t, d, dlq, broker.ReplayOptions{Selection: broker.Selection{Filter: overflowed, Limit: 1, Scan: 3}, To: orders}, nil,
				"m1 → "+skipped, "m2 → "+orders)
			waitForDepth(t, ch, dlq, 2)
			if ids := browseIDs(t, d, dlq, 2); !slices.Equal(ids, []string{"m1", "orphan"}) {
				t.Fatalf("%s holds %q after a limited replay, want m1 and orphan", dlq, ids)
			}
			waitForDepth(t, ch, orders, 3)
			if ids := browseIDs(t, d, orders, 3); !slices.Equal(ids, []string{"m0", "m3", "m2"}) {
				t.Fatalf("%s holds %q, want the replayed m0, m3 and m2", orders, ids)
			}
		})
	}
}

func TestRemove(t *testing.T) {
	for _, queueType := range []string{"classic", "quorum"} {
		t.Run(queueType, func(t *testing.T) {
			d, ch := connect(t)
			dlq := deadLetter(t, ch, testQueue(queueType), queueType, 5)

			var backedUp, got []string
			opts := broker.RemoveOptions{
				Selection: broker.Selection{Filter: broker.Filter{IDs: broker.Rule[string]{Any: []string{"m1", "m3"}}}, Scan: 5},
				Backup:    func(m broker.Message) error { backedUp = append(backedUp, m.ID); return nil },
			}
			err := broker.Remove(context.Background(), d.(broker.Taker), dlq, opts, func(r broker.Taken) { got = append(got, outcome(r)) })
			skipped := " →  ✗ " + broker.ErrSkipped.Error()
			if want := []string{"m0" + skipped, "m1 → ", "m2" + skipped, "m3 → ", "m4" + skipped}; err != nil || !slices.Equal(got, want) {
				t.Fatalf("Remove() = %q, %v; want %q", got, err, want)
			}
			if !slices.Equal(backedUp, []string{"m1", "m3"}) {
				t.Errorf("backed up %q, want m1 and m3", backedUp)
			}

			waitForDepth(t, ch, dlq, 3)
			if ids := browseIDs(t, d, dlq, 3); !slices.Equal(ids, []string{"m0", "m2", "m4"}) {
				t.Fatalf("%s holds %q after a filtered remove, want the rest in order", dlq, ids)
			}
		})
	}
}

func TestPublishAfterClosedChannel(t *testing.T) {
	ctx := context.Background()
	d, ch := connect(t)
	queue := testQueue("classic")
	declare(t, ch, queue, "classic", amqp.Table{})

	p := d.(broker.Publisher)
	if err := p.Publish(ctx, queue, broker.Message{ID: "bad", Properties: map[string]any{"expiration": "soon"}}); err == nil {
		t.Fatal("Publish() of an invalid expiration = nil, want the broker's error")
	}
	if err := p.Publish(ctx, queue, broker.Message{ID: "good"}); err != nil {
		t.Fatalf("Publish() after the broker closed the channel = %v, want nil", err)
	}
	waitForDepth(t, ch, queue, 1)
}

func TestPurge(t *testing.T) {
	for _, queueType := range []string{"classic", "quorum"} {
		t.Run(queueType, func(t *testing.T) {
			d, ch := connect(t)
			source := testQueue(queueType)
			dlq := deadLetter(t, ch, source, queueType, 3)

			if err := d.(broker.Purger).Purge(context.Background(), dlq); err != nil {
				t.Fatal(err)
			}
			waitForDepth(t, ch, dlq, 0)

			missing := source + ".missing"
			if err := d.(broker.Purger).Purge(context.Background(), missing); err == nil || err.Error() != `queue "`+missing+`" not found` {
				t.Errorf("purge of a missing queue: err = %v", err)
			}
		})
	}
}

func firstN(n int) broker.ReplayOptions {
	return broker.ReplayOptions{Selection: broker.Selection{Scan: n}}
}

func replay(t *testing.T, d broker.Driver, queue string, opts broker.ReplayOptions, wantErr error, want ...string) {
	t.Helper()
	var got []string
	err := broker.Replay(context.Background(), d.(broker.Replayer), queue, opts, func(r broker.Taken) {
		got = append(got, outcome(r))
	})
	if !errors.Is(err, wantErr) || !slices.Equal(got, want) {
		t.Fatalf("Replay(%+v) = %q, %v; want %q, %v", opts, got, err, want, wantErr)
	}
}

func outcome(r broker.Taken) string {
	s := r.Message.ID + " → " + r.To
	if r.Err != nil {
		s += " ✗ " + r.Err.Error()
	}

	return s
}

func browseIDs(t *testing.T, d broker.Driver, queue string, n int) []string {
	t.Helper()
	var ids []string
	if err := d.Browse(context.Background(), queue, n, func(m broker.Message) bool { ids = append(ids, m.ID); return true }); err != nil {
		t.Fatal(err)
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
	declare(t, ch, dlq, queueType, amqp.Table{})
	declare(t, ch, source, queueType, deadLettersTo(dlq))

	msgs := make([]amqp.Publishing, n)
	for i := range msgs {
		msgs[i] = amqp.Publishing{MessageId: "m" + strconv.Itoa(i), DeliveryMode: amqp.Persistent, AppId: "limbo-test", Body: []byte(`{"n":` + strconv.Itoa(i) + `}`)}
	}
	reject(t, ch, source, msgs...)
	waitForDepth(t, ch, dlq, n)

	return dlq
}

func declare(t *testing.T, ch *amqp.Channel, name, queueType string, args amqp.Table) {
	t.Helper()
	args["x-queue-type"] = queueType
	if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = ch.QueueDelete(name, false, false, false) })
}

func deadLettersTo(dlq string) amqp.Table {
	return amqp.Table{"x-dead-letter-exchange": "", "x-dead-letter-routing-key": dlq}
}

func reject(t *testing.T, ch *amqp.Channel, queue string, msgs ...amqp.Publishing) {
	t.Helper()
	for _, msg := range msgs {
		publish(t, ch, queue, msg)
	}
	waitForDepth(t, ch, queue, len(msgs))

	for range msgs {
		d, ok, err := ch.Get(queue, false)
		if err != nil || !ok {
			t.Fatalf("get from %s: ok %v, err %v", queue, ok, err)
		}
		if err := d.Reject(false); err != nil {
			t.Fatal(err)
		}
	}
}

func publish(t *testing.T, ch *amqp.Channel, queue string, msg amqp.Publishing) {
	t.Helper()
	if err := ch.PublishWithContext(context.Background(), "", queue, false, false, msg); err != nil {
		t.Fatal(err)
	}
}

// Quorum queues apply returns asynchronously, so depth settles shortly after a browse
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
