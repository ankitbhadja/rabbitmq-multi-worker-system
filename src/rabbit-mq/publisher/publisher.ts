import { config } from "../config";
import amqp, { ConfirmChannel, Connection } from "amqplib";
import { SignupEvent } from "../signup-event";

export class Publisher {
  private connection: Connection | null = null;

  private channel: ConfirmChannel | null = null;

  private timer: NodeJS.Timeout | null = null;

  private running = false;

  async init(): Promise<void> {
    this.connection = await amqp.connect(config.amqpUrl);
    this.channel = await this.connection.createConfirmChannel();
    await this.channel.assertExchange(config.exchangeName, config.type, {
      durable: true,
    });
    await this.channel.assertExchange(config.exchangeTopic, config.typeTopic, {
      durable: true,
    });
    await this.channel.assertQueue(config.workerQueueName, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": config.deadLetterExchange,
        "x-dead-letter-routing-key": "user.signup",
      },
    });
    await this.channel.bindQueue(
      config.workerQueueName,
      config.exchangeTopic,
      "user.signup",
    );
    await this.channel.assertExchange(config.deadLetterExchange, "direct", {
      durable: true,
    });
    await this.channel.assertQueue(config.deadLetterQueue, { durable: true });
    await this.channel.bindQueue(
      config.deadLetterQueue,
      config.deadLetterExchange,
      "user.signup",
    );

    this.running = true;
    let i = 1;
    const publishNext = async (): Promise<void> => {
      if (!this.running) return;
      try {
        await this.publishTopic({ userId: i, action: "signup" });
        i += 1;
      } catch (error) {
        if (this.running) {
          console.error("Failed to publish signup event:", error);
        }
        this.running = false;
        return;
      }
      if (this.running) {
        this.timer = setTimeout(() => void publishNext(), 2000);
      }
    };
    void publishNext();
  }

  async close(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    try {
      await this.channel?.close();
    } finally {
      try {
        await this.connection?.close();
      } finally {
        this.channel = null;
        this.connection = null;
      }
    }
  }

  async publishMessage(event: SignupEvent): Promise<void> {
    if (!this.channel) {
      throw new Error("Publisher has not been initialized");
    }
    const messageBuffer = Buffer.from(JSON.stringify(event));
    this.channel.publish(config.exchangeName, "", messageBuffer, {
      persistent: true,
      contentType: "application/json",
    });
    await this.channel.waitForConfirms();
    console.log("Message published:", event);
  }

  private async publishTopic(event: SignupEvent): Promise<void> {
    if (!this.channel) {
      throw new Error("Publisher has not been initialized");
    }
    const messageBuffer = Buffer.from(JSON.stringify(event));
    this.channel.publish(config.exchangeTopic, "user.signup", messageBuffer, {
      persistent: true,
      contentType: "application/json",
    });
    await this.channel.waitForConfirms();
    console.log("Message published:", "user.signup", event);
  }
}
