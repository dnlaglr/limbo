package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dnlaglr/limbo/internal/broker"
)

const dialTimeout = 30 * time.Second

type driver struct {
	conn *amqp.Connection
}

func Open(ctx context.Context, u *url.URL) (broker.Driver, error) {
	conn, err := amqp.DialConfig(u.String(), amqp.Config{Dial: dialer(ctx)})
	if err != nil {
		return nil, err
	}

	return &driver{conn: conn}, nil
}

func (d *driver) Close() error { return d.conn.Close() }

// Messages stay unacked until one multiple nack returns them all in order
// Closing the channel instead would count as a failed delivery against a quorom queue's delivery limit
func (d *driver) Peek(ctx context.Context, queue string, limit int) (msgs []broker.Message, err error) {
	ch, err := d.conn.Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	var last uint64
	defer func() {
		if last > 0 {
			err = errors.Join(err, ch.Nack(last, true, true))
		}
	}()

	for len(msgs) < limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		delivery, ok, err := ch.Get(queue, false)
		if err != nil {
			return nil, queueError(queue, err)
		}
		if !ok {
			break
		}

		last = delivery.DeliveryTag
		msgs = append(msgs, toMessage(queue, &delivery))
	}

	return msgs, nil
}

func dialer(ctx context.Context) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		if err := conn.SetDeadline(time.Now().Add(dialTimeout)); err != nil {
			conn.Close()
			return nil, err
		}

		return conn, nil
	}
}

func queueError(queue string, err error) error {
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) && amqpErr.Code == amqp.NotFound {
		return fmt.Errorf("queue %q not found", queue)
	}

	return err
}

func toMessage(queue string, d *amqp.Delivery) broker.Message {
	headers := maps.Clone(map[string]any(d.Headers))
	maps.DeleteFunc(headers, func(k string, _ any) bool {
		return k == "x-death" || k == "x-delivery-count" || strings.HasPrefix(k, "x-first-death-") || strings.HasPrefix(k, "x-last-death-")
	})

	return broker.Message{
		ID:            d.MessageId,
		Queue:         queue,
		Body:          d.Body,
		ContentType:   d.ContentType,
		Timestamp:     d.Timestamp,
		DeliveryCount: toInt(d.Headers["x-delivery-count"]),
		Headers:       headers,
		Deaths:        toDeaths(d.Headers["x-death"]),
	}
}

func toDeaths(v any) []broker.Death {
	entries, _ := v.([]any)
	deaths := make([]broker.Death, 0, len(entries))
	for _, entry := range entries {
		t, ok := entry.(amqp.Table)
		if !ok {
			continue
		}

		queue, _ := t["queue"].(string)
		reason, _ := t["reason"].(string)
		exchange, _ := t["exchange"].(string)
		at, _ := t["time"].(time.Time)
		deaths = append(deaths, broker.Death{
			Queue:       queue,
			Reason:      reason,
			Count:       toInt(t["count"]),
			Time:        at,
			Exchange:    exchange,
			RoutingKeys: toStrings(t["routing-keys"]),
		})
	}

	return deaths
}

func toInt(v any) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case int32:
		return int(n)
	}

	return 0
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	strs := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			strs = append(strs, s)
		}
	}

	return strs
}
