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

const (
	dialTimeout    = 30 * time.Second
	confirmTimeout = 30 * time.Second

	replayCountHeader = "x-limbo-replay-count"
	replayedAtHeader  = "x-limbo-replayed-at"
)

type driver struct {
	conn *amqp.Connection
	out  *publisher
}

func Open(ctx context.Context, u *url.URL) (broker.Driver, error) {
	conn, err := amqp.DialConfig(u.String(), amqp.Config{Dial: dialer(ctx)})
	if err != nil {
		return nil, err
	}

	return &driver{conn: conn}, nil
}

func (d *driver) Close() error { return d.conn.Close() }

func (d *driver) Browse(ctx context.Context, queue string, n int, visit func(broker.Message) bool) error {
	return d.Take(ctx, queue, n, func(m broker.Message, _ func() error) (bool, error) {
		return visit(m), nil
	})
}

func (d *driver) Depth(_ context.Context, queue string) (int, error) {
	ch, err := d.conn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()

	q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	if err != nil {
		return 0, queueError(queue, err)
	}

	return q.Messages, nil
}

// The broker closes a channel on some publish errors, such as an oversized message, so a closed one is reopened for the next message
func (d *driver) Publish(_ context.Context, queue string, m broker.Message) error {
	if d.out == nil || d.out.ch.IsClosed() {
		out, err := d.publisher()
		if err != nil {
			return err
		}
		d.out = out
	}

	return d.out.publish(queue, republishing(m, time.Now()))
}

func (d *driver) Purge(_ context.Context, queue string) error {
	ch, err := d.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueuePurge(queue, false)
	return queueError(queue, err)
}

// Deliveries visit doesn't remove stay unacked until one multiple nack returns them all in order
// Closing the channel instead would count as a failed delivery against a quorum queue's delivery limit
// TODO: One basic.get round trip per message, a prefetching consumer is the upgrade if big scans get too slow
func (d *driver) Take(ctx context.Context, queue string, n int, visit func(broker.Message, func() error) (bool, error)) (err error) {
	ch, err := d.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	var held uint64
	defer func() {
		if held > 0 && !ch.IsClosed() {
			err = errors.Join(err, ch.Nack(held, true, true))
		}
	}()

	var stopped error
	visiting, inRequeued := true, false
	for range n {
		if visiting && ctx.Err() != nil {
			visiting, stopped = false, ctx.Err()
		}
		if !visiting && (!inRequeued || ch.IsClosed()) {
			return stopped
		}

		delivery, ok, err := ch.Get(queue, false)
		if err != nil {
			return errors.Join(stopped, queueError(queue, err))
		}
		if !ok {
			return stopped
		}

		inRequeued = requeued(&delivery)
		if !visiting {
			held = delivery.DeliveryTag
			continue
		}

		acked := false
		more, err := visit(toMessage(queue, &delivery), func() error {
			acked = true
			return ch.Ack(delivery.DeliveryTag, false)
		})
		if !acked {
			held = delivery.DeliveryTag
		}
		if err != nil || !more {
			visiting, stopped = false, err
		}
	}

	return stopped
}

// Quorum queues serve requeued messages first and in the order they came back, so a scan that stops among them holds the rest too, or its nack would put what it read behind them
// They carry x-acquired-count from RabbitMQ 4.3 and x-delivery-count before it, classic queues requeue in place and set neither
func requeued(d *amqp.Delivery) bool {
	return d.Redelivered && (d.Headers["x-acquired-count"] != nil || d.Headers["x-delivery-count"] != nil)
}

type publisher struct {
	ch      *amqp.Channel
	returns chan amqp.Return
	closed  chan *amqp.Error
}

func (d *driver) publisher() (*publisher, error) {
	ch, err := d.conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, err
	}

	return &publisher{
		ch:      ch,
		returns: ch.NotifyReturn(make(chan amqp.Return, 1)),
		closed:  ch.NotifyClose(make(chan *amqp.Error, 1)),
	}, nil
}

// Confirm wait ignores cancellation because giving up on it would leave the message's fate unknown
// RabbitMQ sends a return before the confirm of the same message, so it's already buffered once the confirm arrives
func (p *publisher) publish(queue string, msg amqp.Publishing) error {
	ctx, cancel := context.WithTimeout(context.Background(), confirmTimeout)
	defer cancel()

	confirm, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, msg)
	if err != nil {
		return err
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("no confirm from the broker within %s, it may have arrived anyway: %w", confirmTimeout, err)
	}
	if !acked {
		select {
		case e := <-p.closed:
			if e != nil {
				return e
			}
		default:
		}
		return errors.New("the broker refused it")
	}

	select {
	case <-p.returns:
		return fmt.Errorf("no queue named %q", queue)
	default:
		return nil
	}
}

func republishing(m broker.Message, now time.Time) amqp.Publishing {
	headers := withoutDeathHeaders(m.Headers)
	if headers == nil {
		headers = amqp.Table{}
	}
	delete(headers, "x-acquired-count")
	headers[replayCountHeader] = int64(toInt(m.Headers[replayCountHeader]) + 1)
	headers[replayedAtHeader] = now

	str := func(k string) string { s, _ := m.Properties[k].(string); return s }
	num := func(k string) uint8 { n, _ := m.Properties[k].(uint8); return n }

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     m.ContentType,
		ContentEncoding: str("content_encoding"),
		DeliveryMode:    num("delivery_mode"),
		Priority:        num("priority"),
		CorrelationId:   str("correlation_id"),
		ReplyTo:         str("reply_to"),
		Expiration:      str("expiration"),
		MessageId:       m.ID,
		Timestamp:       m.Timestamp,
		Type:            str("type"),
		UserId:          str("user_id"),
		AppId:           str("app_id"),
		Body:            m.Body,
	}
}

// Properties hold what limbo doesn't model but needs to publish an identical copy, from a file as well as from the queue
func properties(d *amqp.Delivery, headers amqp.Table) map[string]any {
	p := map[string]any{
		"content_encoding": d.ContentEncoding,
		"delivery_mode":    d.DeliveryMode,
		"priority":         d.Priority,
		"correlation_id":   d.CorrelationId,
		"reply_to":         d.ReplyTo,
		"expiration":       d.Expiration,
		"type":             d.Type,
		"user_id":          d.UserId,
		"app_id":           d.AppId,
	}
	maps.DeleteFunc(p, func(_ string, v any) bool { return v == "" || v == uint8(0) })
	if types := headerTypes(headers); len(types) > 0 {
		p["header_types"] = types
	}
	if len(p) == 0 {
		return nil
	}

	return p
}

// JSON keeps strings, booleans, tables and arrays but not number widths, decimals, timestamps or bytes, so those get their AMQP type recorded
// TODO: Top-level headers only, values nested in tables and arrays stay best-effort until a broker needs them exact
func headerTypes(headers amqp.Table) map[string]string {
	types := map[string]string{}
	for k, v := range headers {
		switch v.(type) {
		case int8, uint8, int16, uint16, int32, uint32, int64, float32, float64:
			types[k] = fmt.Sprintf("%T", v)
		case amqp.Decimal:
			types[k] = "decimal"
		case time.Time:
			types[k] = "timestamp"
		case []byte:
			types[k] = "bytes"
		}
	}

	return types
}

// Peek shows these as fields and replayed copies would mislead
// This is because RabbitMQ 4.x passes them through untouched
func withoutDeathHeaders(headers amqp.Table) amqp.Table {
	clean := maps.Clone(headers)
	maps.DeleteFunc(clean, func(k string, _ any) bool {
		return k == "x-death" || k == "x-delivery-count" || strings.HasPrefix(k, "x-first-death-") || strings.HasPrefix(k, "x-last-death-")
	})

	return clean
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
	headers := withoutDeathHeaders(d.Headers)
	return broker.Message{
		ID:            d.MessageId,
		Queue:         queue,
		Body:          d.Body,
		ContentType:   d.ContentType,
		Timestamp:     d.Timestamp,
		DeliveryCount: toInt(d.Headers["x-delivery-count"]),
		Headers:       headers,
		Properties:    properties(d, headers),
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
