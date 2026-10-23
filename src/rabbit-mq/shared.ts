// rabbitmq-service.ts
import amqp, { Channel, Connection } from "amqplib";

export class RabbitMQService {
  private connection: Connection | null = null;

  private channel: Channel | null = null;

  async connect(url: string): Promise<void> {
    if (this.connection) return;

    this.connection = await amqp.connect(url);
    this.channel = await this.connection.createChannel();
  }

  async createChannel(): Promise<Channel> {
    if (!this.connection || !this.channel) {
      throw new Error("RabbitMQ connection and channel are not established");
    }
    return this.channel;
  }

  async close(): Promise<void> {
    try {
      await this.channel?.close();
    } finally {
      try {
        await this.connection?.close();
      } finally {
        this.connection = null;
        this.channel = null;
      }
    }
  }
}
