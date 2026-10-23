# RabbitMQ multiple workers

This TypeScript demo publishes signup events to a RabbitMQ topic exchange.
Three worker processes consume from the same durable queue, so RabbitMQ
distributes each event to one available worker (competing consumers).

## Technical concepts: messaging with RabbitMQ

RabbitMQ is a **message broker**: it accepts messages from producers, routes
them to queues, and delivers them to consumers. The publisher and workers do
not need to run at the same time or call each other directly. This separates
event creation from event processing and lets work wait in a queue when no
worker is connected.

### Exchange, routing key, and queue

The publisher sends each signup event to the durable `user_events` **topic
exchange** with the routing key `user.signup`. An exchange is the routing
entry point for messages; it does not store them. The durable
`user_signup_workers` queue is bound to that exchange with the `user.signup`
binding pattern, so matching signup events are placed in the queue. A topic
exchange can route using patterns such as `user.*` or `user.#`; this demo uses
an exact routing key and binding. The publisher declares the queue and binding
before it starts sending, so messages can be queued even if workers have not
connected yet.

### Competing consumers and acknowledgements

All three worker processes consume from the same queue. They are **competing
consumers**, not three separate subscribers each receiving a copy: RabbitMQ
delivers each queued event to one available worker. Each worker sets a
prefetch limit of one unacknowledged event. After processing an event, it
sends a manual acknowledgement (`ack`), allowing RabbitMQ to dispatch more
work to it. Manual acknowledgements let the broker know that a delivery was
processed; if a worker disconnects before acknowledging, RabbitMQ can requeue
the unacknowledged delivery for another worker.

### Durability and delivery guarantees

The exchange and queue are declared durable, and the publisher marks messages
persistent. These settings allow RabbitMQ to recover topology and queued
messages after a broker restart, subject to the broker's persistence and
deployment configuration. They do not by themselves provide end-to-end
exactly-once processing: a worker may process an event and disconnect before
its acknowledgement reaches RabbitMQ, causing a redelivery. Production
consumers should therefore make event handling idempotent, and publishers
typically use publisher confirms to learn whether the broker accepted a
message. This publisher uses a confirm channel and waits for the broker's
confirmation before sending the next event. Malformed events are rejected
without requeueing and routed to the durable
`user_signup_workers.dead_letter` queue through a dead-letter exchange,
avoiding an infinite poison-message retry loop. Workers also validate that
events contain an integer `userId` and the expected `signup` action before
acknowledging them.

### Practices and production considerations

This demo reuses a publisher connection/channel instead of opening a new
connection for every event, declares durable topology, marks messages
persistent, waits for publisher confirms, applies consumer prefetch, manually
acknowledges successfully handled events, and routes rejected events to a
dead-letter queue. Startup is asynchronous and reports initialization errors.

It is still an educational example, not a complete production messaging
system. It does not automatically reconnect after a broker or channel failure,
use TLS or externalized credentials, validate a versioned event schema,
implement idempotent business processing, or configure dead-letter retention
and monitoring. A publisher confirm means the broker accepted the message; it
does not independently guarantee that a message was routed to a queue or
processed by a worker. Production systems should also consider mandatory
publishing/returned-message handling, connection recovery, observability, and
appropriate broker durability/replication settings.

The demo handles `SIGINT` and `SIGTERM` by closing its channels and
connections. For production use, shutdown should also stop accepting new work
and allow in-flight processing to finish within a bounded timeout.

The worker queue now has dead-letter arguments. If an older version already
created `user_signup_workers` without those arguments, RabbitMQ will reject
the redeclaration. Drain or otherwise account for any queued messages before
intentionally deleting and recreating that queue with the new configuration.

## Requirements

- Node.js 18 or later
- npm
- Docker, or an existing RabbitMQ server

## Start RabbitMQ with Docker

```sh
docker run -d --name poc-rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```

The management dashboard is available at <http://localhost:15672> (default
credentials: `guest` / `guest`).

## Install and run

In the `rabbit-mq` directory:

```sh
npm install
```

Open three terminals in this directory. Run the following in each:

```sh
# Terminal 1: worker 1 and the event publisher
npm start

# Terminal 2: worker 2
npm run worker:2

# Terminal 3: worker 3
npm run worker:3
```

Each command builds the TypeScript project before starting. The publisher
sends a signup event every two seconds; check the terminal output to see which
worker receives each event.

If RabbitMQ is not at the default URL, set `AMQP_URL` before starting the
processes. For example:

```sh
AMQP_URL=amqp://user:password@localhost:5672 npm start
```

Workers acknowledge valid events after handling them. Rejected messages are
sent to the dead-letter queue; inspect that queue in the RabbitMQ dashboard.

Stop the Docker container when finished:

```sh
docker stop poc-rabbitmq
```
