package rabbitmq

import (
	"context"
	"os"
	"reflect"
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

// Requires a broker, e.g. LIMBO_TEST_AMQP_URL=amqp://guest:guest@localhost:5672/
func TestPeekLeavesMessages(t *testing.T) {
	rawURL := os.Getenv("LIMBO_TEST_AMQP_URL")
	if rawURL == "" {
		t.Skip("LIMBO_TEST_AMQP_URL is not set")
	}
	u, err := broker.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}

	for _, queueType := range []string{"classic", "quorum"} {
		t.Run(queueType, func(t *testing.T) {
			ctx := context.Background()
			ch := channel(t, rawURL)
			source := "limbo.test." + queueType + "." + strconv.FormatInt(time.Now().UnixNano(), 36)
			dlq := deadLetter(t, ch, source, queueType, 3)

			d, err := Open(ctx, u)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { d.Close() })

			for range 3 {
				msgs, err := d.Peek(ctx, dlq, 2)
				if err != nil {
					t.Fatal(err)
				}
				if len(msgs) != 2 || msgs[0].ID != "m0" || msgs[1].ID != "m1" {
					t.Fatalf("peeked %+v, want m0 and m1 in order", msgs)
				}
				if deaths := msgs[0].Deaths; len(deaths) != 1 || deaths[0].Queue != source || deaths[0].Reason != "expired" {
					t.Fatalf("deaths = %+v, want one expiry from %s", deaths, source)
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

// Publishes n messages to an instantly expiring queue so they land in the DLQ with an x-death header
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
	declare(source, amqp.Table{"x-message-ttl": 0, "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dlq})

	for i := range n {
		msg := amqp.Publishing{MessageId: "m" + strconv.Itoa(i), Body: []byte(`{"n":` + strconv.Itoa(i) + `}`)}
		if err := ch.PublishWithContext(context.Background(), "", source, false, false, msg); err != nil {
			t.Fatal(err)
		}
	}

	waitForDepth(t, ch, dlq, n)

	return dlq
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

func channel(t *testing.T, rawURL string) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	return ch
}

func depth(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	return q.Messages
}
