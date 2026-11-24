package sqs

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dnlaglr/limbo/internal/broker"
)

const (
	holdTimeout   = 5 * time.Minute
	pollWait      = 1
	settleTimeout = 30 * time.Second
	maxBatch      = 10
	maxAttributes = 10

	deathReason     = "delivery_limit"
	groupHeader     = "MessageGroupId"
	attributeTypes  = "attribute_types"
	replayCountAttr = "x-limbo-replay-count"
	replayedAtAttr  = "x-limbo-replayed-at"
)

// Copies holds the IDs of messages sent during the current Take, so it never hands back a copy that failed again and returned during the run
type driver struct {
	client *awssqs.Client
	urls   map[string]string
	copies map[string]bool
}

func Open(ctx context.Context, u *url.URL) (broker.Driver, error) {
	endpoint, err := parseURL(u)
	if err != nil {
		return nil, err
	}

	var opts []func(*config.LoadOptions) error
	if u.Host != "" {
		opts = append(opts, config.WithRegion(u.Host))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		return nil, errors.New("no AWS region: use sqs://<region> or set AWS_REGION")
	}
	if _, err := cfg.Credentials.Retrieve(ctx); err != nil {
		return nil, fmt.Errorf("loading AWS credentials: %w", err)
	}

	client := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
		}
	})

	return &driver{client: client, urls: map[string]string{}, copies: map[string]bool{}}, nil
}

func parseURL(u *url.URL) (endpoint string, err error) {
	switch {
	case u.User != nil:
		return "", errors.New("credentials come from the AWS environment, a profile or SSO, not the URL")
	case u.Port() != "":
		return "", errors.New("sqs://<region> takes no port, pass ?endpoint=http://host:port to reach an emulator")
	case u.Opaque != "" || u.Path != "" && u.Path != "/":
		return "", errors.New("use sqs://<region>, optionally with ?endpoint=<url>")
	}

	query := u.Query()
	for k := range query {
		if k != "endpoint" {
			return "", fmt.Errorf("unknown SQS URL option %q (only endpoint is supported)", k)
		}
	}

	endpoint = query.Get("endpoint")
	if endpoint == "" {
		return "", nil
	}
	if e, err := url.Parse(endpoint); err != nil || e.User != nil || e.Host == "" || e.Scheme != "http" && e.Scheme != "https" {
		return "", errors.New("endpoint must be a URL such as http://localhost:9324, without credentials")
	}

	return endpoint, nil
}

func (d *driver) Close() error { return nil }

func (d *driver) Browse(ctx context.Context, queue string, n int, visit func(broker.Message) bool) error {
	return d.Take(ctx, queue, n, func(m broker.Message, _ func() error) (bool, error) {
		return visit(m), nil
	})
}

func (d *driver) Depth(ctx context.Context, queue string) (int, error) {
	addr, err := d.queueURL(ctx, queue)
	if err != nil {
		return 0, err
	}

	out, err := d.client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       &addr,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
}

func (d *driver) Publish(ctx context.Context, queue string, m broker.Message) error {
	ctx, cancel := settle(ctx)
	defer cancel()

	addr, err := d.queueURL(ctx, queue)
	if err != nil {
		return err
	}

	out, err := d.client.SendMessage(ctx, sendInput(addr, queue, m, time.Now()))
	if err != nil {
		return err
	}
	d.copies[aws.ToString(out.MessageId)] = true

	return nil
}

func (d *driver) Purge(ctx context.Context, queue string) error {
	ctx, cancel := settle(ctx)
	defer cancel()

	addr, err := d.queueURL(ctx, queue)
	if err != nil {
		return err
	}

	_, err = d.client.PurgeQueue(ctx, &awssqs.PurgeQueueInput{QueueUrl: &addr})
	if _, ok := errors.AsType[*types.PurgeQueueInProgress](err); ok {
		return errors.New("only one purge per SQS queue is allowed every 60 seconds, try again in a minute")
	}

	return err
}

func sendInput(addr, to string, m broker.Message, now time.Time) *awssqs.SendMessageInput {
	dataTypes := broker.StringMap(m.Properties[attributeTypes])
	attrs := map[string]types.MessageAttributeValue{}
	for k, v := range m.Headers {
		if k != groupHeader && k != replayCountAttr && k != replayedAtAttr {
			attrs[k] = attribute(dataTypes[k], v)
		}
	}

	if len(attrs)+2 <= maxAttributes {
		attrs[replayCountAttr] = attribute("Number", strconv.Itoa(atoi(fmt.Sprint(m.Headers[replayCountAttr]))+1))
		attrs[replayedAtAttr] = attribute("String", now.UTC().Format(time.RFC3339))
	}

	in := &awssqs.SendMessageInput{QueueUrl: &addr, MessageBody: aws.String(string(m.Body)), MessageAttributes: attrs}
	if group, _ := m.Headers[groupHeader].(string); group != "" {
		in.MessageGroupId = &group
	}

	// Stable for this dead-letter copy so a rerun can't send it twice
	if strings.HasSuffix(to, ".fifo") {
		in.MessageDeduplicationId = aws.String("limbo-" + m.ID)
	}

	return in
}

// A message read back from a file holds Binary values as base64 strings, and one from another broker any JSON value, so each
// becomes the closest attribute and a recorded data type always wins
func attribute(dataType string, v any) types.MessageAttributeValue {
	text := func(s, fallback string) types.MessageAttributeValue {
		return types.MessageAttributeValue{DataType: aws.String(cmp.Or(dataType, fallback)), StringValue: &s}
	}

	switch v := v.(type) {
	case []byte:
		return types.MessageAttributeValue{DataType: aws.String(cmp.Or(dataType, "Binary")), BinaryValue: v}
	case string:
		if !strings.HasPrefix(dataType, "Binary") {
			return text(v, "String")
		}
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			b = []byte(v)
		}
		return types.MessageAttributeValue{DataType: &dataType, BinaryValue: b}
	case json.Number:
		return text(v.String(), "Number")
	case map[string]any, []any:
		b, _ := json.Marshal(v)
		return text(string(b), "String")
	}

	return text(fmt.Sprint(v), "String")
}

func (d *driver) queueURL(ctx context.Context, name string) (string, error) {
	if addr, ok := d.urls[name]; ok {
		return addr, nil
	}

	out, err := d.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: &name})
	if _, ok := errors.AsType[*types.QueueDoesNotExist](err); ok {
		return "", fmt.Errorf("queue %q not found", name)
	}
	if err != nil {
		return "", err
	}

	d.urls[name] = aws.ToString(out.QueueUrl)
	return d.urls[name], nil
}

// Every message received stays hidden until release returns it, so SQS can't hand it out twice, and one it hands out again anyway is visited only once
// A receive that brings nothing new ends the scan, as on an empty queue or a FIFO queue whose groups are all held
func (d *driver) Take(ctx context.Context, queue string, n int, visit func(broker.Message, func() error) (bool, error)) (err error) {
	addr, err := d.queueURL(ctx, queue)
	if err != nil {
		return err
	}

	clear(d.copies)
	held := map[string]string{}
	defer func() { err = errors.Join(err, d.release(ctx, addr, held)) }()

	var stopped error
	visiting, visited := true, 0
	for visiting && visited < n {
		if err := ctx.Err(); err != nil {
			return err
		}

		msgs, err := d.receive(ctx, addr, min(maxBatch, n-visited))
		if err != nil {
			return err
		}

		fresh := false
		for _, m := range msgs {
			id, handle := aws.ToString(m.MessageId), aws.ToString(m.ReceiptHandle)
			if prev, seen := held[id]; seen {
				if prev == "" {
					_ = d.remove(ctx, addr, handle)
				} else {
					held[id] = handle
				}
				continue
			}

			held[id], fresh = handle, true
			if d.copies[id] {
				continue
			}
			if visiting && ctx.Err() != nil {
				visiting, stopped = false, ctx.Err()
			}
			if !visiting {
				continue
			}

			visited++
			more, err := visit(toMessage(queue, m), func() error {
				if err := d.remove(ctx, addr, held[id]); err != nil {
					return err
				}
				held[id] = ""
				return nil
			})
			if err != nil || !more {
				visiting, stopped = false, err
			}
		}
		if !fresh {
			break
		}
	}

	return stopped
}

// Calls that settle a message's fate ignore cancellation, so Ctrl-C never strands a message hidden or half replayed
func settle(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
}

func (d *driver) receive(ctx context.Context, addr string, n int) ([]types.Message, error) {
	ctx, cancel := settle(ctx)
	defer cancel()

	in := &awssqs.ReceiveMessageInput{
		QueueUrl:                    &addr,
		MaxNumberOfMessages:         int32(n),
		VisibilityTimeout:           int32(holdTimeout / time.Second),
		WaitTimeSeconds:             pollWait,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames:       []string{"All"},
	}
	// The SDK resends the same attempt ID when it retries, so FIFO queues return a batch whose response was lost instead of hiding it
	if strings.HasSuffix(addr, ".fifo") {
		in.ReceiveRequestAttemptId = aws.String(rand.Text())
	}

	out, err := d.client.ReceiveMessage(ctx, in)
	if err != nil {
		return nil, err
	}

	return out.Messages, nil
}

func (d *driver) remove(ctx context.Context, addr, handle string) error {
	ctx, cancel := settle(ctx)
	defer cancel()

	_, err := d.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: &addr, ReceiptHandle: &handle})
	return err
}

// TODO: One batch at a time, ~30s per 10k held messages, send chunks in parallel if big scans need it
func (d *driver) release(ctx context.Context, addr string, held map[string]string) error {
	var handles []string
	for _, h := range held {
		if h != "" {
			handles = append(handles, h)
		}
	}

	failed, cause := 0, error(nil)
	for chunk := range slices.Chunk(handles, maxBatch) {
		n, err := d.unhide(ctx, addr, chunk)
		failed += n
		cause = cmp.Or(cause, err)
	}
	if failed > 0 {
		return fmt.Errorf("could not return %d of the held messages, SQS shows them again within %d minutes: %w", failed, int(holdTimeout.Minutes()), cause)
	}

	return nil
}

func (d *driver) unhide(ctx context.Context, addr string, handles []string) (failed int, err error) {
	ctx, cancel := settle(ctx)
	defer cancel()

	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(handles))
	for i, h := range handles {
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(h)}
	}

	out, err := d.client.ChangeMessageVisibilityBatch(ctx, &awssqs.ChangeMessageVisibilityBatchInput{QueueUrl: &addr, Entries: entries})
	if err != nil {
		return len(handles), err
	}

	for _, f := range out.Failed {
		// A hold that ran out, or a message another consumer received since, is no longer limbo's to return
		if code := aws.ToString(f.Code); strings.HasSuffix(code, "MessageNotInflight") || strings.HasSuffix(code, "ReceiptHandleIsInvalid") {
			continue
		}
		failed++
		if err == nil {
			err = errors.New(cmp.Or(aws.ToString(f.Message), aws.ToString(f.Code)))
		}
	}

	return failed, err
}

func toMessage(queue string, m types.Message) broker.Message {
	msg := broker.Message{
		ID:            aws.ToString(m.MessageId),
		Queue:         queue,
		Body:          []byte(aws.ToString(m.Body)),
		Timestamp:     epochMillis(m.Attributes["SentTimestamp"]),
		DeliveryCount: max(atoi(m.Attributes["ApproximateReceiveCount"])-1, 0),
		Headers:       headers(m),
		Properties:    properties(m),
	}
	if arn := m.Attributes["DeadLetterQueueSourceArn"]; arn != "" {
		msg.Deaths = []broker.Death{{Queue: arn[strings.LastIndexByte(arn, ':')+1:], Reason: deathReason, Count: 1}}
	}

	return msg
}

func headers(m types.Message) map[string]any {
	h := map[string]any{}
	for k, v := range m.MessageAttributes {
		if strings.HasPrefix(aws.ToString(v.DataType), "Binary") {
			h[k] = v.BinaryValue
		} else {
			h[k] = aws.ToString(v.StringValue)
		}
	}
	if group := m.Attributes[groupHeader]; group != "" {
		h[groupHeader] = group
	}
	if len(h) == 0 {
		return nil
	}

	return h
}

// Headers carry every attribute's value as a string or bytes, so a copy needs each one's data type to match the original
func properties(m types.Message) map[string]any {
	dataTypes := map[string]string{}
	for k, v := range m.MessageAttributes {
		if t := aws.ToString(v.DataType); t != "String" {
			dataTypes[k] = t
		}
	}
	if len(dataTypes) == 0 {
		return nil
	}

	return map[string]any{attributeTypes: dataTypes}
}

func epochMillis(s string) time.Time {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}

	return time.UnixMilli(ms).UTC()
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
