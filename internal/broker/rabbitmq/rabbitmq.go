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

func (d *driver) Replay(ctx context.Context, queue string, opts broker.ReplayOptions, each func(broker.Replayed)) (err error) {
	in, err := d.conn.Channel()
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := d.publisher()
	if err != nil {
		return err
	}
	defer out.ch.Close()

	var held uint64
	defer func() {
		if held > 0 && !in.IsClosed() {
			err = errors.Join(err, in.Nack(held, true, true))
		}
	}()

	for range opts.Limit {
		if err := ctx.Err(); err != nil {
			return err
		}

		delivery, ok, err := in.Get(queue, false)
		if err != nil {
			return queueError(queue, err)
		}
		if !ok {
			return nil
		}

		r := broker.Replayed{Message: toMessage(queue, &delivery)}
		if r.To, r.Err = opts.Target(r.Message); r.Err == nil {
			r.Err = out.publish(r.To, &delivery)
		}

		var ackErr error
		if r.Err == nil {
			ackErr = in.Ack(delivery.DeliveryTag, false)
		} else {
			held = delivery.DeliveryTag
		}
		each(r)

		switch {
		case ackErr != nil:
			return ackErr
		case out.ch.IsClosed() || errors.Is(r.Err, context.DeadlineExceeded):
			return r.Err
		}
	}

	return nil
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
func (p *publisher) publish(queue string, d *amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(context.Background(), confirmTimeout)
	defer cancel()

	confirm, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, republishing(d, time.Now()))
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

func republishing(d *amqp.Delivery, now time.Time) amqp.Publishing {
	headers := withoutDeathHeaders(d.Headers)
	if headers == nil {
		headers = amqp.Table{}
	}
	delete(headers, "x-acquired-count")
	headers[replayCountHeader] = int64(toInt(d.Headers[replayCountHeader]) + 1)
	headers[replayedAtHeader] = now

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserId:          d.UserId,
		AppId:           d.AppId,
		Body:            d.Body,
	}
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
	return broker.Message{
		ID:            d.MessageId,
		Queue:         queue,
		Body:          d.Body,
		ContentType:   d.ContentType,
		Timestamp:     d.Timestamp,
		DeliveryCount: toInt(d.Headers["x-delivery-count"]),
		Headers:       withoutDeathHeaders(d.Headers),
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
