const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "notifications-service",
  brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "notifications-group" });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  console.log("Notifications Service connected, waiting for events...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      console.log(`Sending email notification for order #${order.id}`);
    }
  });
}

start().catch(console.error);
