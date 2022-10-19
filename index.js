import { createClient } from "redis";
const client = createClient();
let counter = 2724;

// log events
client.on("error", (err) => console.error(err));
client.on("connect", () => console.log("client connecting"));
client.on("ready", () => console.log("client connected"));
client.on("end", () => console.log("client disconnected"));
client.on("reconnecting", () => console.log("client reconnecting"));

// handle ctrl+c
process.on("SIGINT", async () => {
  console.log("SIGINT, Waiting for client to finish...");
  await client.quit();
  process.exit(0);
});

await client.connect();

setInterval(async () => {
  // add message to stream
  await client.xAdd(
    "mystream", // name of the stream
    "*", // id
    // Payload to add to the stream:
    {
      ts: new Date().valueOf().toString()
    },
    {
      // how many messages to retain in a stream
      TRIM: {
        strategy: "MAXLEN", // Trim by length.
        strategyModifier: "~", // Approximate trimming.
        threshold: 1000, // Retain around 1000 entries.
      },
    }
  );
  const streamInfo = await client.xInfoStream("mystream");
  console.log(streamInfo);
}, 10);
