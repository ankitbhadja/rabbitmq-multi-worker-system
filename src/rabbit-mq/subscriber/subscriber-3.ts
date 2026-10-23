import { config } from "../config";
import { Channel, ConsumeMessage } from "amqplib";
import { RabbitMQService } from "../shared";
import { parseSignupEvent } from "../signup-event";

const rabbitMQService = new RabbitMQService();

export class Subscriber3 {
  channel: Channel | null = null;

  async close(): Promise<void> {
    await rabbitMQService.close();
    this.channel = null;
  }

  async init() {
    await rabbitMQService.connect(config.amqpUrl);
    const channel = await rabbitMQService.createChannel();
    this.channel = channel;

    await channel.assertExchange(config.exchangeName, config.type, {
      durable: true,
    });

    const queue = await channel.assertQueue("subscriber_3", {
      durable: true,
    });

    await channel.bindQueue(queue.queue, config.exchangeName, "");

    await channel.consume(
      queue.queue,
      (msg: ConsumeMessage | null) => {
        if (msg) {
          const message = parseSignupEvent(msg.content);
          console.log("Subscriber3 received message:", message);
          // this.channel.ack(msg); // Acknowledge message
        }
      },
      { noAck: true },
    );
  }

  async initTopic() {
    await rabbitMQService.connect(config.amqpUrl);
    const channel = await rabbitMQService.createChannel();
    this.channel = channel;

    await channel.assertExchange(config.exchangeTopic, config.typeTopic, {
      durable: true,
    });

    const queue = await channel.assertQueue(config.workerQueueName, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": config.deadLetterExchange,
        "x-dead-letter-routing-key": "user.signup",
      },
    });
    await channel.assertExchange(config.deadLetterExchange, "direct", {
      durable: true,
    });
    const deadLetterQueue = await channel.assertQueue(config.deadLetterQueue, {
      durable: true,
    });
    await channel.bindQueue(
      deadLetterQueue.queue,
      config.deadLetterExchange,
      "user.signup",
    );

    await channel.bindQueue(
      queue.queue,
      config.exchangeTopic,
      "user.signup",
    );

    await channel.prefetch(1);
    await channel.consume(
      queue.queue,
      (msg: ConsumeMessage | null) => {
        if (msg) {
          try {
            const message = JSON.parse(msg.content.toString());
            console.log(
              "Subscriber3 received message:",
              msg.fields.consumerTag,
              message,
            );
            channel.ack(msg);
          } catch (error) {
            console.error("Failed to process signup event:", error);
            channel.nack(msg, false, false);
          }
        }
      },
      { noAck: false },
    );
  }
}
