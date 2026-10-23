import { Subscriber2 } from "./subscriber/subscriber-2";

const start2 = async () => {
  const worker = new Subscriber2();
  const handleShutdown = () => {
    void worker.close().catch((error) => {
      console.error("Failed to close RabbitMQ worker 2:", error);
      process.exitCode = 1;
    });
  };
  process.once("SIGINT", handleShutdown);
  process.once("SIGTERM", handleShutdown);
  await worker.initTopic();
};

void start2().catch((error) => {
  console.error("Failed to start RabbitMQ worker 2:", error);
  process.exitCode = 1;
});
