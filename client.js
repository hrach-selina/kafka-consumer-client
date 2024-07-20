const { Kafka } = require("kafkajs");

const brokersStr = process.env.KAFKA_BROKERS;

if(!brokersStr) {
  throw new Error("KAFKA_BROKERS is required");
}

const brokers = brokersStr.split(",");

exports.kafka = new Kafka({
  clientId: "local-client",
  brokers,
});
