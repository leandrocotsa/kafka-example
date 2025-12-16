const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "orders-service",
  brokers: ["localhost:9092"]
});

const producer = kafka.producer();

async function start() {
  await producer.connect();
  console.log("Orders Service Producer connected to Kafka");

  // Endpoint to create a new order
  app.post("/orders", async (req, res) => {
    const { product, quantity } = req.body;
    if (!product || !quantity) {
      return res.status(400).json({ error: "Product and quantity are required" });
    }

    const order = {
      id: Math.floor(Math.random() * 10000),
      product,
      quantity
    };

    try {
      await producer.send({
        topic: "orders",
        messages: [
          { value: JSON.stringify(order) }
        ]
      });

      console.log("OrderCreated event sent:", order);
      res.status(201).json({ message: "Order created", order });
    } catch (err) {
      console.error("Failed to send order event", err);
      res.status(500).json({ error: "Failed to create order" });
    }
  });

  app.listen(3000, () => {
    console.log("Orders Service running on port 3000");
  });
}

start().catch(console.error);
