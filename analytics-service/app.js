const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "analytics-service",
  brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "analytics-group" });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  console.log("Analytics Service connected, waiting for events...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      console.log(`Analytics recorded order #${order.id} for product ${order.product}`);
    }
  });
}

start().catch(console.error);
