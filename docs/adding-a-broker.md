# Adding a broker

limbo reaches every broker through the interfaces in `internal/broker`. To add a broker, you write a driver package
that implements them and register a URL scheme for it. The `peek`, `stats`, `export`, `replay`, `delete` and `purge`
commands, filters included, then work with the new broker without changes.

The RabbitMQ driver (`internal/broker/rabbitmq`) and the SQS driver (`internal/broker/sqs`) are the reference
implementations.

## How a command reaches a driver

1. The command parses `--url` or `LIMBO_URL` with `broker.ParseURL`.
2. It looks up the URL scheme in `app.App.Brokers` and calls that broker's `broker.Opener`.
3. It calls `Depth` to count the queue, then `Browse` to read it. Commands that remove messages go through
   `broker.Replay` or `broker.Remove`, which call your `Take` and `Publish`. `purge --no-backup` calls `Purge`.

limbo matches filters itself, because no supported broker can filter a queue server-side. A driver never inspects
filters. `broker.Replay` and `broker.Remove` apply them to each message your `Take` hands out.

## What a driver implements

| Interface | Methods | Required | Unlocks |
|---|---|---|---|
| `broker.Driver` | `Browse`, `Depth`, `Close` | Yes | `peek`, `stats`, `export` |
| `broker.Taker` | `Take` | No | `delete`, `purge` |
| `broker.Publisher` | `Publish` | No | `replay`, together with `Taker` |
| `broker.Purger` | `Purge` | No | `purge --no-backup` as one fast call, instead of removing messages one by one |

`broker.Replayer` is `Taker` and `Publisher` together. limbo finds each capability by type assertion, so a driver
that can't remove or publish messages safely leaves that interface out, and the commands that need it report that the
scheme is unsupported. A driver implements primitives only. The rules every broker shares, such as removing a message
only after its copy or backup is safe, live once in `internal/broker`.

## Choose the model for your dead-letter queue

How you implement `Browse` and `Take` depends on how the broker lets you read a message without removing it.

| Model | Example | Read without removing | Return what was read | Remove (`Take`) |
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

- Hold each message for a fixed time, and release every held message before `Take` returns, including after Ctrl-C.
- The broker can hand out a message twice. Track the IDs you have visited and skip repeats.
- End the scan when a receive brings nothing new. This ends it on an empty queue, and on FIFO message groups that the
  broker locks while you hold their messages.
- Don't report a release failure for a hold that already expired. The message is visible again.

### Readable store

- `Browse` is a range read, so it has nothing to return.
- `Depth` is usually exact, for example `XLEN` on a Redis stream.
- `Take`'s remove deletes the message by its ID.

### Log

- `Browse` reads each partition from its start offset without committing, so it has nothing to return.
- A log has no per-message delete. Leave out `Taker`, and with it `replay`, `delete` and `purge`, until you decide
  what removal means for the broker. Committing a consumer-group offset, for example, removes only a prefix of each
  partition. `Publisher` doesn't depend on removal, so a log can still implement it.

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

- The CLI prints only the scheme, host and path of the URL. Don't accept secrets in the host or path.
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
| `Properties` | Anything else `Publish` needs to send an identical copy, such as RabbitMQ's delivery mode or SQS attribute data types, or nil |
| `Deaths` | Dead-letter history, most recent first |

`Properties` keys are snake_case and broker-specific. Export and backup files store them as JSON, and only your driver
reads them back, so record what an identical copy needs and nothing more. If a header or attribute value has a type
that JSON can't express, record its type too, as the RabbitMQ driver's `header_types` and the SQS driver's
`attribute_types` do.

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

Write the read loop once, as the `Take` method, and build `Browse` on it. `Take` hands `visit` each message with a
function that removes it, and returns every message `visit` doesn't remove, exactly as `Browse` does:

```go
func (d *driver) Browse(ctx context.Context, queue string, n int, visit func(broker.Message) bool) error {
	return d.Take(ctx, queue, n, func(m broker.Message, _ func() error) (bool, error) {
		return visit(m), nil
	})
}
```

### Step 6: Implement Take and Publish

Skip this step if the broker has no safe way to remove a single message. Without `Take`, the driver still supports
`peek`, `stats` and `export`.

`broker.Replay` and `broker.Remove` own the rules that every broker shares. They skip messages the filters reject,
resolve the destination, remove a message only after its copy is sent or its backup is on disk, and stop at the
limit. Your driver supplies the read loop with its remove, and a confirmed publish:

```go
func (d *driver) Take(ctx context.Context, queue string, n int, visit func(broker.Message, func() error) (bool, error)) error {
	held := d.client.Hold(queue)
	defer held.ReturnAll()

	for range n {
		r, ok, err := held.Next(ctx)
		if err != nil || !ok {
			return err
		}

		more, err := visit(toMessage(queue, r), func() error { return d.remove(ctx, r) })
		if err != nil || !more {
			return err
		}
	}

	return nil
}

func (d *driver) Publish(ctx context.Context, queue string, m broker.Message) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	return d.client.PublishConfirmed(ctx, queue, replayCopy(m, time.Now()))
}
```

When you implement `Take`, its remove, `Publish` and the copy:

- Read no more than `n` messages. Replay and delete pass the depth counted before the run, so messages that fail again
  and return to the dead-letter queue aren't taken twice. If a copy you published can return within that count, as on
  SQS, remember the IDs of the copies published during the current `Take` and skip them.
- Return nil from `Publish` only after the broker confirms it has the copy. Return an error that wraps
  `context.DeadlineExceeded` when you gave up waiting for that confirm, because the copy may have arrived anyway.
  `broker.Replay` then ends the run instead of risking more of the same.
- Build the copy from the `broker.Message` alone, using `Headers` and `Properties`, so it doesn't depend on where the
  message came from. Drop the broker's dead-letter metadata. Set `x-limbo-replay-count` to the previous value plus
  one, and set `x-limbo-replayed-at` to the current time.
- Run the remove and `Publish` under `context.WithoutCancel(ctx)` with a timeout, as `Publish` does above, so Ctrl-C
  can't leave a message half removed or half replayed.
- If the destination deduplicates, as SQS FIFO queues do, derive the deduplication ID from the dead-letter copy's ID.
  Never reuse the original's.

Return an error from `Take` only when the run can't continue, such as a lost connection, or when `visit` returns one.
Per-message outcomes are reported by `broker.Replay` and `broker.Remove`, not by your driver.

### Step 7: Implement Purge (optional)

If the broker can empty a queue in one call, implement `broker.Purger` so `purge --no-backup` doesn't have to remove
messages one by one:

```go
func (d *driver) Purge(ctx context.Context, queue string) error {
	return d.client.Purge(ctx, queue)
}
```

Return `queue "<name>" not found` for a missing queue, as `Depth` does. The CLI help and README describe what the
native purge does on each broker, such as which messages it skips and how long it takes.

### Step 8: Register the scheme

Add the opener to the registry in `internal/app/app.go`:

```go
Brokers: broker.Registry{
	"amqp":     rabbitmq.Open,
	"amqps":    rabbitmq.Open,
	"sqs":      sqs.Open,
	"mybroker": mybroker.Open,
},
```

### Step 9: Update the help text

The commands have no broker-specific code, but their help text describes what each broker does to messages. In
`internal/cli`:

- Add an example URL to `urlUsage` in `peek.go`.
- Describe what a read does on the broker, such as redelivery counts or hidden messages, in `readHelp` in
  `selection.go`.
- Add any new death reasons to the `--reason` flag help in `selection.go`, and say what a message ID is on the broker
  in `filterHelp` if it isn't unique.
- Describe how replayed copies differ from the originals in the `replay` help in `replay.go`.
- If you implement `Purge`, describe what it does in the `purge` help in `delete.go`.

### Step 10: Test the driver

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
- A replay to the origin queue moves the messages, and the copies carry the limbo headers and the original properties.
- A replay to a missing queue, a canceled replay and a filtered replay leave the remaining messages in the queue.
- A message without dead-letter history fails with `broker.ErrNoOrigin` unless `To` is set.
- A filtered `broker.Remove` backs up and removes only the matching messages, and leaves the rest in order.
- `Purge`, if implemented, empties the queue and reports a missing queue.

Run these through `broker.Replay` and `broker.Remove`, as the CLI does, so they cover your driver together with the
shared rules.

Start the broker or an emulator, then run:

```bash
LIMBO_TEST_MYBROKER_URL=mybroker://localhost:7000 go test -race ./internal/broker/mybroker/
```

CI runs the integration tests in the `integration` job in `.github/workflows/ci.yml`. Add the broker or emulator there
as a service with a health check, and set your environment variable in the job's `env`. The job fails when any
integration test skips, so a missing variable can't pass unnoticed.

## Checklist

- [ ] `Open` rejects invalid URLs and never prints secrets.
- [ ] `toMessage` sets `Deaths` with the queue the message failed in first, and `Properties` with what a copy needs.
- [ ] `Browse` and `Take` return every held message on every path.
- [ ] `Publish`, if implemented, returns nil only after the broker confirms the copy, and builds it from the message
  alone.
- [ ] `Purge`, if implemented, is described in the `purge` help and the README.
- [ ] The scheme is registered in `internal/app/app.go`.
- [ ] The help text describes what the broker does to messages.
- [ ] The broker runs as a service in the CI `integration` job, with its environment variable set.
- [ ] `task test` and `task lint` pass, and the integration tests pass against the broker.
