import { Subscriber3 } from "./subscriber/subscriber-3";

const start3 = async () => {
  const worker = new Subscriber3();
  const handleShutdown = () => {
    void worker.close().catch((error) => {
      console.error("Failed to close RabbitMQ worker 3:", error);
      process.exitCode = 1;
    });
  };
  process.once("SIGINT", handleShutdown);
  process.once("SIGTERM", handleShutdown);
  await worker.initTopic();
};

void start3().catch((error) => {
  console.error("Failed to start RabbitMQ worker 3:", error);
  process.exitCode = 1;
});
