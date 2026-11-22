<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/assets/banner-dark.png">
    <img alt="limbo" src=".github/assets/banner-light.png">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/dnlaglr/limbo/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/dnlaglr/limbo"></a>
  <a href="https://github.com/dnlaglr/limbo/releases"><img alt="Downloads" src="https://img.shields.io/github/downloads/dnlaglr/limbo/total"></a>
  <a href="https://github.com/dnlaglr/limbo/releases/latest"><img alt="Platforms" src="https://img.shields.io/badge/platform-macOS%20%7C%20Windows%20%7C%20Linux-blue"></a>
  <a href="https://github.com/dnlaglr/limbo/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/dnlaglr/limbo/actions/workflows/ci.yml/badge.svg"></a>
</p>

**Get failed messages out of limbo. Safely.**

A single binary for inspecting, triaging, and replaying dead-letter queue messages, with the same workflow for
RabbitMQ and Amazon SQS. More brokers are planned.

- `limbo peek` shows what is in a dead-letter queue and leaves every message there.
- `limbo stats` counts the messages and tells you when and why they died.
- `limbo replay` moves messages back to the queue they failed in, removing each one only after the broker confirms
  its copy.

## Install

Download the archive for your platform from the [latest release](https://github.com/dnlaglr/limbo/releases/latest).
Archives are named `limbo_<version>_<os>_<arch>`, for `darwin`, `linux` and `windows` on `amd64` and `arm64`, as
`.tar.gz` or `.zip` on Windows. Each release has a `checksums.txt` to verify them against:

```bash
VERSION=0.1.0
curl -fsSLO "https://github.com/dnlaglr/limbo/releases/download/v${VERSION}/limbo_${VERSION}_linux_amd64.tar.gz"
curl -fsSLO "https://github.com/dnlaglr/limbo/releases/download/v${VERSION}/checksums.txt"
sha256sum --check --ignore-missing checksums.txt
tar -xzf "limbo_${VERSION}_linux_amd64.tar.gz" limbo
sudo install limbo /usr/local/bin/
```

The binaries are not signed or notarized. If macOS refuses to open a downloaded binary, run
`xattr -d com.apple.quarantine limbo`.

With Go 1.27.4 or later:

```bash
go install github.com/dnlaglr/limbo/cmd/limbo@latest
```

## Quickstart

```bash
export LIMBO_URL=amqp://guest:guest@localhost:5672/

limbo stats orders.dlq
limbo peek orders.dlq -n 5
limbo replay orders.dlq --reason rejected --dry-run
limbo replay orders.dlq --reason rejected
```

`stats` and `peek` print what they found and put every message back. The dry run shows where each message would go.
The last command asks before it moves anything.

## Broker URLs

Pass the broker with `--url` or set `LIMBO_URL`. Limbo prints only the scheme, host and path of the URL, never the
credentials.

### RabbitMQ

```
amqp://user:pass@host:5672/vhost
amqps://user:pass@host:5671/vhost
```

`amqps://` connects over TLS. The path is the virtual host, URL-encoded. Leave it out for the default vhost `/`.

### Amazon SQS

```
sqs://eu-west-1
sqs://
sqs://us-east-1?endpoint=http://localhost:9324
```

The host is the AWS region. Leave it out to use `AWS_REGION` or your profile's region. `endpoint` points limbo at an
emulator such as ElasticMQ, or at another SQS endpoint.

Credentials come from the standard AWS chain: environment variables, a shared profile (`AWS_PROFILE`), SSO, or a
container or instance role. Limbo rejects credentials in the URL. Name queues by their name, not their URL. Limbo looks
them up in the account the credentials belong to.

## Commands

### peek

```bash
limbo peek orders.dlq
limbo peek orders.dlq --reason rejected --from orders --newer-than 1h
limbo peek orders-dlq.fifo --url sqs://eu-west-1 --header MessageGroupId=acme
limbo peek orders.dlq -n 50 -o json | jq .body
```

Shows up to `--limit` (`-n`, default 10) matching messages: ID, publish time, content type, delivery count, dead-letter
history, headers and body. It reads until it has enough matches or reaches the end of the queue as counted when it
started, then returns every message it read.

### stats

```bash
limbo stats orders.dlq
limbo stats orders.dlq --from orders --newer-than 1h -o json | jq .reasons
```

Reads every message in the queue, as counted when it starts, and reports how many there are, when the oldest and newest
died, and how many died for each reason. Press Ctrl-C to stop early and see the counts so far.

### replay

```bash
limbo replay orders.dlq --dry-run
limbo replay orders.dlq --reason rejected --from orders --newer-than 1h
limbo replay orders.dlq --to orders.v2 -n 100 --rate 50 --yes
```

Publishes messages again to the queue each one died in, or to `--to`, and removes each one from the dead-letter queue
once the broker confirms its copy. Messages with no dead-letter history stay where they are unless you pass `--to`.

| Flag | Effect |
|---|---|
| `--dry-run` | Show where each message would go without moving anything |
| `-y`, `--yes` | Replay without asking first. Required without a terminal |
| `--to <queue>` | Replay into this queue instead of the one each message died in |
| `-n`, `--limit <n>` | Replay at most this many matching messages (default all) |
| `--rate <n>` | Replay at most this many messages per second (default unlimited) |

Limbo asks before it replays. With filters, it reads the queue once to count the matches before it asks, and then
replays no more than the messages it counted.

### Common flags

| Flag | Effect |
|---|---|
| `-o`, `--output text\|json` | Output format for `peek`, `stats` and `replay` |
| `-q`, `--quiet` | Print only results and errors, without status lines or warnings |
| `--no-color` | Disable colored output, same as `NO_COLOR=1` |
| `--debug` | Print debug logs to stderr |

`limbo version` shows the version, commit and build date. A build from `go install` shows only the version, because Go
records no commit or date for it.

## Filters

`peek`, `stats` and `replay` take the same filters. They pick messages by their last death, headers, body and age.

| Filter | Matches messages |
|---|---|
| `--reason <reason>` | whose last death had this reason: `rejected`, `expired`, `maxlen` or `delivery_limit` |
| `--from <queue>` | that last died in this queue |
| `--header <key>` or `<key>=<value>` | with this header, or this value for it |
| `--body <regex>` | whose body matches this [regular expression](https://pkg.go.dev/regexp/syntax) |
| `--older-than <age>` | that died longer ago than this, such as `30m`, `2h` or `7d` |
| `--newer-than <age>` | that died within this long |

`--reason`, `--from`, `--header` and `--body` each have a `--not-` form that skips the messages it matches.

Different filters must all match. A filter given more than once matches any of its values, and `--not-` filters skip
every message they match. So `--reason rejected --reason expired --not-from payments` picks messages that were rejected
or expired, in any queue except `payments`.

Age is the time of the last death, or the publish time when the broker records no death time, as on SQS. On SQS every
dead-lettered message has the reason `delivery_limit`, message attributes are its headers, and FIFO messages also have
a `MessageGroupId` header.

Limbo matches filters itself, because neither broker can filter a queue. A filtered run reads messages until it has
enough matches, which can mean every message up to the depth it counted.

## JSON output

With `-o json`, results go to stdout and status lines to stderr, so you can pipe the output straight into `jq`.

`peek` prints one message per line. `body_encoding` is `utf-8`, or `base64` for a binary body:

```json
{"id":"o1","queue":"orders.dlq","content_type":"application/json","timestamp":"2026-11-20T18:57:10Z","delivery_count":2,"headers":{"tenant":"acme"},"deaths":[{"queue":"orders","reason":"rejected","count":1,"time":"2026-11-20T18:57:11Z","exchange":"shop","routing_keys":["orders"]}],"body":"{\"orderId\":1042}","body_encoding":"utf-8"}
```

`stats` prints one object. `partial` is `true` when the read stopped before the end of the queue, and is left out
otherwise:

```json
{"queue":"orders.dlq","depth":6,"scanned":6,"matched":6,"oldest":"2026-11-19T18:57:13Z","newest":"2026-11-20T18:57:12Z","reasons":{"expired":2,"none":1,"rejected":3}}
```

`replay` prints one record per message, with the status `replayed`, `returned`, `would_replay` or `would_return`:

```json
{"id":"o1","to":"orders","status":"replayed"}
{"id":"orphan","status":"returned","error":"no dead-letter history, pass --to to choose a destination"}
```

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success, including an empty queue or a replay you declined |
| `1` | An error: invalid flags or URL, a connection or broker error, a replay that left messages it could not replay, or a replay without `--yes` and without a terminal |
| `130` | Interrupted by Ctrl-C or SIGTERM. Limbo returns the messages it holds before it exits |

## Safety

### What limbo guarantees

- `peek` and `stats` never remove a message. Limbo holds every message it reads and returns them all when it is done,
  including after Ctrl-C.
- `replay` removes a message from the dead-letter queue only after the broker confirms its copy reached the
  destination. Messages it cannot replay, skips or doesn't reach go back to the dead-letter queue.
- `replay` never reads past the depth it counted before it started, so messages that fail again and return to the
  dead-letter queue are not replayed twice in one run.
- Limbo never prints the credentials in the broker URL.

### Caveats

- **Replay is at least once.** If the connection drops after the broker confirms a copy but before limbo removes the
  original, the message ends up in both queues. Consumers should tolerate duplicates.
- **Reading a message is a delivery.** Brokers have no way to read a queue without handing out its messages, so the
  messages look delivered afterwards. See the broker sections below.
- **Other consumers of the dead-letter queue don't see the messages limbo holds** until it returns them.
- **A second Ctrl-C quits at once**, without returning the messages limbo holds. RabbitMQ returns them when the
  connection closes, and SQS shows them again when their 5-minute hold runs out.

#### RabbitMQ

- Limbo gets each message without acknowledging it and returns them all with one requeue when it is done. RabbitMQ
  marks the returned messages as redelivered.
- Classic queues put returned messages back in their original positions. Quorum queues put them at the head of the
  queue, in their original order.
- Quorum queues before RabbitMQ 4.3 count each read toward their delivery limit, so a peek can push a message past
  it. If limbo's connection drops, RabbitMQ returns the held messages itself, and quorum queues count that as a
  delivery.
- Replayed messages drop the broker's `x-death`, `x-first-death-*`, `x-last-death-*`, `x-delivery-count` and
  `x-acquired-count` headers and carry `x-limbo-replay-count` and `x-limbo-replayed-at` instead.

#### Amazon SQS

- Each read adds one to a message's receive count. If the dead-letter queue has a redrive policy of its own, limbo's
  reads count toward its `maxReceiveCount` and can move messages on.
- Limbo hides each message it reads for 5 minutes and makes it visible again when it is done. If limbo is killed, the
  messages reappear when the 5 minutes run out.
- A read or replay that runs longer than 5 minutes can end early, because the first messages limbo holds become visible
  again. Limbo warns when it reads fewer messages than it counted.
- The queue depth SQS reports is approximate, so a read can end short of it. Standard queues hand out messages in no
  fixed order, so a replay with `--limit` may pick other matching messages than the dry run listed.
- SQS allows about 120,000 messages in flight on a standard queue, and limbo holds every message it reads until it is
  done, so one run can't read more than that.
- FIFO queues hand out at most the first 10 messages of each message group while limbo holds them, so limbo can't
  reach the rest of a group in one run.
- Replayed copies are new messages with new IDs. They keep the body, message attributes and message group, and carry
  `x-limbo-replay-count` and `x-limbo-replayed-at` unless that would pass the limit of 10 attributes.
- Copies sent to a FIFO queue use `limbo-` followed by the dead-letter copy's message ID as their deduplication ID, so
  rerunning a replay within the 5-minute deduplication window doesn't send a copy twice.

### SQS permissions

`peek` and `stats` need the first statement. `replay` also needs the other two, with a statement like the last one for
every queue it sends to:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LimboRead",
      "Effect": "Allow",
      "Action": ["sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:ReceiveMessage", "sqs:ChangeMessageVisibility"],
      "Resource": "arn:aws:sqs:eu-west-1:123456789012:orders-dlq"
    },
    {
      "Sid": "LimboRemoveReplayed",
      "Effect": "Allow",
      "Action": "sqs:DeleteMessage",
      "Resource": "arn:aws:sqs:eu-west-1:123456789012:orders-dlq"
    },
    {
      "Sid": "LimboReplayTo",
      "Effect": "Allow",
      "Action": ["sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:SendMessage"],
      "Resource": "arn:aws:sqs:eu-west-1:123456789012:orders"
    }
  ]
}
```

A destination needs `sqs:GetQueueAttributes` only with `--to`, which limbo checks before it starts. For queues
encrypted with a customer managed KMS key, limbo also needs `kms:Decrypt` on the key to read, and `kms:GenerateDataKey`
and `kms:Decrypt` to send.

## Contributing

To add a broker, read [Adding a broker](docs/adding-a-broker.md). It covers the driver interfaces, how each kind of
broker maps onto them, and how to test against a real broker.

`task test` runs the tests and `task lint` runs the lint checks from CI. The integration tests run when
`LIMBO_TEST_AMQP_URL` or `LIMBO_TEST_SQS_URL` points at a broker. CI runs them against RabbitMQ and ElasticMQ.

## License

[Apache 2.0](LICENSE)
