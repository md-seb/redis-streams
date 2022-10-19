import { createClient, commandOptions } from "redis";
import Debug from "debug";
const debug = Debug("consumer");

const msgs = new Set();
const msgs2 = new Set();

const streamName = 'mystream';
const createConsumer = async (streamName, groupName, consumerName, handler) => {
  const client = createClient();
  client.on("error", (err) => console.error("client error", streamName, groupName, consumerName, err));
  client.on("connect", () => console.log("client connecting", streamName, groupName, consumerName));
  client.on("ready", () => console.log("client connected", streamName, groupName, consumerName));
  client.on("end", () => console.log("client disconnected", streamName, groupName, consumerName));
  client.on("reconnecting", () => console.log("client reconnecting", streamName, groupName, consumerName));

  // let isShuttingDown = false;
  // // handle ctrl+c
  // process.on("SIGINT", async () => {
  //   isShuttingDown = true;
  //   console.log("SIGINT, Waiting for client to finish...");
  //   await client.quit();
  //   process.exit(0);
  // });

  await client.connect();

  // Create the consumer group (and stream) if needed...
  try {
    await client.xGroupCreate(streamName, groupName, "$", {
      MKSTREAM: false,
    });
    console.log("Created consumer group.");
  } catch (err) {
    console.log("xGroupCreate error", streamName, groupName, consumerName, err);
  }

  while (true) {
    try {
      // read from the stream
      let res = await client.xReadGroup(
        commandOptions({
          // when using isolated: true,
          // the client throws lots of errors on quitting,
          // potentially hanging the process.
        }),
        groupName,
        consumerName,
        [
          {
            key: streamName,
            id: ">", // Next entry ID that no consumer in this group has read
          },
        ],
        {
          COUNT: 1, // read 1 entry at a time
          BLOCK: 1000, // block for 1 second if no entries
        }
      );

      if (res) {
        res.forEach((stream) => {
          stream.messages.forEach(async (message) => {
            debug(
              'read',
              stream.name,
              groupName,
              consumerName,
              message.id,
              message.message.ts
            );
            handler(message.message.ts);
            await client.xAck(streamName, groupName, message.id);
            debug(" ack", stream.name, groupName, consumerName, message.id);
          });
        });
      }
    } catch (err) {
      console.log("xReadGroup error", streamName, groupName, consumerName, err);
    }
  }
};

//create x consumers
[1, 2, 3, 4, 5].forEach((i) => {
  const groupName = 'g1';
  const consumerName = `c${i.toString()}`;
  createConsumer(streamName, groupName, consumerName, (ts) => {
    // message handler
    const tsi = parseInt(ts);
    if (msgs.has(tsi)) {
      console.error('Duplicate', tsi);
    } else {
      msgs.add(tsi);
    }
    console.log(streamName, groupName, consumerName, tsi);
  });
});

[1, 2, 3, 4, 5].forEach((i) => {
  const groupName = 'g2';
  const consumerName = `c${i.toString()}`;
  createConsumer(streamName, groupName, consumerName, (ts) => {
    // message handler
    const tsi = parseInt(ts);
    if (msgs2.has(tsi)) {
      console.error('Duplicate', tsi);
    } else {
      msgs2.add(tsi);
    }
    console.log(streamName, groupName, consumerName, tsi);
  });
});
