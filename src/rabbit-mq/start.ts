import { Publisher } from "./publisher/publisher";
import { Subscriber1 } from "./subscriber/subscriber-1";

const start = async () => {
  const worker = new Subscriber1();
  const publisher = new Publisher();
  const shutdown = async () => {
    await Promise.all([worker.close(), publisher.close()]);
  };
  const handleShutdown = () => {
    void shutdown().catch((error) => {
      console.error("Failed to close RabbitMQ connections:", error);
      process.exitCode = 1;
    });
  };
  process.once("SIGINT", handleShutdown);
  process.once("SIGTERM", handleShutdown);

  try {
    await worker.initTopic();
    await publisher.init();
  } catch (error) {
    await shutdown();
    throw error;
  }
};

void start().catch((error) => {
  console.error("Failed to start RabbitMQ publisher and worker:", error);
  process.exitCode = 1;
});
