export const config = {
  amqpUrl: process.env.AMQP_URL ?? "amqp://localhost:5672",
  exchangeName: "message_broadcast_exchange",
  exchangeTopic: "user_events",
  workerQueueName: "user_signup_workers",
  deadLetterExchange: "user_events.dead_letter",
  deadLetterQueue: "user_signup_workers.dead_letter",
  type: "fanout",
  typeTopic: "topic",
};
