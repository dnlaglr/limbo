# Adding a broker

limbo reaches every broker through the interfaces in `internal/broker`. To add a broker, you write a driver package
that implements them and register a URL scheme for it. The `peek`, `stats` and `replay` commands, filters included,
then work with the new broker without changes.

The RabbitMQ driver (`internal/broker/rabbitmq`) and the SQS driver (`internal/broker/sqs`) are the reference
implementations.

## How a command reaches a driver

1. The command parses `--url` or `LIMBO_URL` with `broker.ParseURL`.
2. It looks up the URL scheme in `app.App.Brokers` and calls that broker's `broker.Opener`.
3. It calls `Depth` to count the queue, then `Browse`, or `Replay` for `limbo replay`.

limbo matches filters itself, because no supported broker can filter a queue server-side. A driver never inspects
filters. It passes them to `broker.ReplayRun`, which applies them during a replay.

## What a driver implements

| Interface | Methods | Required |
|---|---|---|
| `broker.Driver` | `Browse`, `Depth`, `Close` | Yes |
| `broker.Replayer` | `Replay` | No. Without it, `limbo replay` reports that the scheme is unsupported. |

limbo finds `Replayer` by type assertion, so a driver that can't move messages safely leaves it out. Every write
capability is an optional interface found this way, so each broker implements only what it supports.

## Choose the model for your dead-letter queue

How you implement `Browse` and `Replay` depends on how the broker lets you read a message without removing it.

| Model | Example | Read without removing | Return what was read | Remove after replay |
|---|---|---|---|---|
| Lock and requeue | RabbitMQ | Get each message unacknowledged | One requeue for all held messages | Acknowledge |
| Visibility lease | SQS | Receive with a visibility timeout | Set the visibility timeout to 0 | Delete by receipt handle |
| Readable store | Redis streams | Range read (`XRANGE`) | Nothing to return | Delete by ID (`XDEL`) |
| Log | Kafka | Read from an offset without committing | Nothing to return | No per-message delete |

### Lock and requeue

- Hold every message you read until the scan ends, then return them all in one call so they keep their order.
- Check how the broker counts a returned message. RabbitMQ quorum queues count a closed channel as a failed delivery,
  so the RabbitMQ driver returns messages with one `nack` instead of closing the channel.

### Visibility lease

- Hold each message for a fixed time, and release every held message before `scan` returns, including after Ctrl-C.
- The broker can hand out a message twice. Track the IDs you have visited and skip repeats.
- End the scan when a receive brings nothing new. This ends it on an empty queue, and on FIFO message groups that the
  broker locks while you hold their messages.
- Don't report a release failure for a hold that already expired. The message is visible again.

### Readable store

- `Browse` is a range read, so it has nothing to return.
- `Depth` is usually exact, for example `XLEN` on a Redis stream.
- Delete a replayed message by its ID, and only after the send succeeds.

### Log

- `Browse` reads each partition from its start offset without committing, so it has nothing to return.
- A log has no per-message delete. Leave out `Replayer` until you decide what removal means for the broker. Committing
  a consumer-group offset, for example, removes only a prefix of each partition.

## Add the driver

In the examples, `Client`, `Dial`, `Record` and their methods stand for your broker's client library.

### Step 1: Create the package

Create `internal/broker/<name>/<name>.go` with an unexported driver type:

```go
package mybroker

type driver struct {
	client *Client
}

func (d *driver) Close() error { return d.client.Close() }
```

### Step 2: Write the opener

`Open` validates the URL, connects and returns the driver. Fail here, at the Connecting step, for anything the user
can fix: an invalid URL, missing credentials or an unreachable host.

```go
func Open(ctx context.Context, u *url.URL) (broker.Driver, error) {
	for k := range u.Query() {
		if k != "tls" {
			return nil, fmt.Errorf("unknown mybroker URL option %q", k)
		}
	}

	client, err := Dial(ctx, u.Host)
	if err != nil {
		return nil, err
	}

	return &driver{client: client}, nil
}
```

Keep secrets out of the output:

- The CLI prints the URL with `u.Redacted()`, which hides only the password in the user info. Don't accept secrets
  anywhere else in the URL.
- Don't put the raw URL in an error.
- If the broker's SDK has a credential chain, such as AWS, Azure or Google Cloud, reject user info and load credentials
  from the chain. The SQS driver shows how.

### Step 3: Map messages

Convert broker messages in one function, `toMessage`:

| Field | Value |
|---|---|
| `ID` | The broker's message ID |
| `Queue` | The queue being read |
| `Body` | The raw payload |
| `Timestamp` | When the message was first published |
| `DeliveryCount` | Deliveries before this read |
| `Headers` | User headers, without the broker's dead-letter bookkeeping |
| `Deaths` | Dead-letter history, most recent first |

Filters and replay read `Deaths[0]` through `Origin()`, `Reason()` and `DiedAt()`:

- Set `Death.Queue` to the queue the message failed in. Replay sends the message back there.
- Reuse an existing reason when the event is the same: `rejected`, `expired`, `maxlen` or `delivery_limit`.
- Leave `Death.Time` zero if the broker doesn't record when a message died. `DiedAt()` then falls back to `Timestamp`.

```go
func toMessage(queue string, r *Record) broker.Message {
	m := broker.Message{
		ID:        r.ID,
		Queue:     queue,
		Body:      r.Payload,
		Timestamp: r.PublishedAt,
		Headers:   r.Headers,
	}
	if r.SourceQueue != "" {
		m.Deaths = []broker.Death{{Queue: r.SourceQueue, Reason: "delivery_limit", Count: 1}}
	}

	return m
}
```

### Step 4: Implement Depth

Return the number of messages waiting in the queue. An approximate count is acceptable. When a read ends below the
count, the CLI warns, and `stats` marks its counts as partial.

For a missing queue, return `fmt.Errorf("queue %q not found", queue)` so that every broker reports it the same way.

### Step 5: Implement Browse

`Browse` must:

- Visit at most `n` messages, in delivery order where the broker keeps one.
- Leave every message in the queue.
- Stop when `visit` returns false, when `ctx` is done, or when the broker hands out no more messages.
- Return every held message before it returns, on every path, including cancellation.

Write the read loop once, as a `scan` method that `Browse` and `Replay` share. Its callback receives a function that
removes the current message:

```go
func (d *driver) Browse(ctx context.Context, queue string, n int, visit func(broker.Message) bool) error {
	return d.scan(ctx, queue, n, func(r *Record, _ func() error) (bool, error) {
		return visit(toMessage(queue, r)), nil
	})
}
```

### Step 6: Implement Replay

Skip this step if the broker has no safe way to remove a single message.

`broker.ReplayRun` owns the rules that every broker shares. It skips messages the filters reject, resolves the
destination, removes a message only after its copy is sent, and stops at the limit. Your driver supplies `send` and
`remove`:

```go
func (d *driver) Replay(ctx context.Context, queue string, opts broker.ReplayOptions, each func(broker.Replayed)) error {
	run := opts.Start(each)
	return d.scan(ctx, queue, opts.Scan, func(r *Record, remove func() error) (bool, error) {
		_, more, err := run.Next(toMessage(queue, r), func(to string) error { return d.send(ctx, to, r) }, remove)
		return more, err
	})
}

func (d *driver) send(ctx context.Context, to string, r *Record) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	return d.client.PublishConfirmed(ctx, to, replayCopy(r, time.Now()))
}
```

When you implement `send`, `remove` and the copy:

- Return nil from `send` only after the broker confirms it has the copy.
- Drop the broker's dead-letter metadata. Set `x-limbo-replay-count` to the previous value plus one, and set
  `x-limbo-replayed-at` to the current time.
- Run `send` and `remove` under `context.WithoutCancel(ctx)` with a timeout, as `send` does above, so Ctrl-C can't
  leave a message half replayed.
- If the destination deduplicates, as SQS FIFO queues do, derive the deduplication ID from the dead-letter copy. Never
  reuse the original's.

Read no more than `opts.Scan` messages. It holds the depth counted before the run, so messages that fail again and
return to the dead-letter queue aren't replayed twice. If a returning copy can appear within that count, as on SQS,
remember the IDs of the copies you sent and skip them.

Return an error from `Replay` only when the run can't continue, such as a lost connection or a failed `remove`.
Report each message's outcome through `each`, with any per-message error in `Replayed.Err`.

### Step 7: Register the scheme

Add the opener to the registry in `internal/app/app.go`:

```go
Brokers: broker.Registry{
	"amqp":     rabbitmq.Open,
	"amqps":    rabbitmq.Open,
	"sqs":      sqs.Open,
	"mybroker": mybroker.Open,
},
```

### Step 8: Update the help text

The commands have no broker-specific code, but their help text describes what each broker does to messages. In
`internal/cli`:

- Add an example URL to `urlUsage` in `peek.go`.
- Describe what a read does on the broker, such as redelivery counts or hidden messages, in `readHelp` in
  `selection.go`.
- Add any new death reasons to the `--reason` flag help in `selection.go`.
- Describe how replayed copies differ from the originals in the `replay` help in `replay.go`.

### Step 9: Test the driver

Write table tests for the pure functions, `toMessage` and the function that builds the copy.

Gate integration tests on an environment variable, so that `go test ./...` passes without a broker:

```go
// Requires a broker, e.g. LIMBO_TEST_MYBROKER_URL=mybroker://localhost:7000
func connect(t *testing.T) broker.Driver {
	t.Helper()
	raw := os.Getenv("LIMBO_TEST_MYBROKER_URL")
	if raw == "" {
		t.Skip("LIMBO_TEST_MYBROKER_URL is not set")
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

	return d
}
```

Against a real broker or an emulator, test that:

- A browse that stops early leaves every message in the queue, in order where the broker keeps one.
- The origin queue and reason are mapped.
- A missing queue returns `queue "<name>" not found`.
- A replay to the origin queue moves the messages and the copies carry the limbo headers.
- A replay to a missing queue, a canceled replay and a filtered replay leave the remaining messages in the queue.
- A message without dead-letter history fails with `broker.ErrNoOrigin` unless `To` is set.

Start the broker or an emulator, then run:

```bash
LIMBO_TEST_MYBROKER_URL=mybroker://localhost:7000 go test -race ./internal/broker/mybroker/
```

## Checklist

- [ ] `Open` rejects invalid URLs and never prints secrets.
- [ ] `toMessage` sets `Deaths` with the queue the message failed in first.
- [ ] `Browse` returns every held message on every path.
- [ ] `Replay`, if implemented, removes a message only after the broker confirms its copy.
- [ ] The scheme is registered in `internal/app/app.go`.
- [ ] The help text describes what the broker does to messages.
- [ ] `task test` and `task lint` pass, and the integration tests pass against the broker.
