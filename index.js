require('dotenv').config();
const { kafka } = require("./client");
const fs = require("fs");

const topic = "pms.bookings";
const writeFilePath = "./data/checkouts.json";

const consumer = kafka.consumer({ groupId: "local-group" });

function writeToFile(data) {
  fs.appendFile(writeFilePath, `${data}\n`, (err) => {
    if (err) {
      console.log(err);
    }
  });
}

async function init() {
  await consumer.connect();

  writeToFile("[");

  let counter = 0;

  let consumedTopicPartitions = {};
  consumer.on(consumer.events.GROUP_JOIN, async ({ payload }) => {
    const { memberAssignment } = payload;
    consumedTopicPartitions = Object.entries(memberAssignment).reduce(
      (topics, [topic, partitions]) => {
        for (const partition in partitions) {
          console.log(`${topic}-${partition}`);
          topics[`${topic}-${partition}`] = false;
        }
        return topics;
      },
      {},
    );
  });

  let processedBatch = true;
  consumer.on(consumer.events.FETCH_START, async () => {
    console.log("FETCH_START");
    if (processedBatch === false) {
      await consumer.disconnect();
      process.exit(0);
    }

    processedBatch = false;
  });

  /*
   * Now whenever we have finished processing a batch, we'll update `consumedTopicPartitions`
   * and exit if all topic-partitions have been consumed,
   */
  consumer.on(consumer.events.END_BATCH_PROCESS, async ({ payload }) => {
    console.log("END_BATCH_PROCESS");
    const { topic, partition, offsetLag } = payload;
    consumedTopicPartitions[`${topic}-${partition}`] = offsetLag === "0";

    if (
      Object.values(consumedTopicPartitions).every((consumed) =>
        Boolean(consumed),
      )
    ) {
      await consumer.disconnect();
      process.exit(0);
    }

    processedBatch = true;
  });

  await consumer.subscribe({ topics: [topic], fromBeginning: true });

  await consumer.run({
    // partitionsConsumedConcurrently: 1,
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      try {
        const stringMessage = message.value.toString();
        const parsed = JSON.parse(stringMessage);

        if (parsed?.eventName === "BookingCheckOut") {
          console.log("Writing checkout", counter);
          writeToFile(stringMessage);
        } else {
          console.log("Not checkout", counter);
        }
        counter++;
      } catch (e) {}
    },
  });

  // await consumer.seek({
  //   offset: "0",
  //   topic,
  //   partition: 0,
  // });
}

init().catch((e) => console.error(`[example/consumer] ${e.message}`, e));

const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.map((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await consumer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.map((type) => {
  process.once(type, async () => {
    try {
      await consumer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});
