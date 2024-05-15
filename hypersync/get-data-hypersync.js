import { keccak256, toHex } from "viem";
import { HypersyncClient } from "@envio-dev/hypersync-client";
import sqlite3 from "sqlite3";
import { encodeCheckpoint, EVENT_TYPES } from "./checkpoint.js"; // Adjusted to import from the new JavaScript file

// Event signatures and address
const event_signatures = [
  "Trade(address,address,bool,uint256,uint256,uint256,uint256,uint256)",
];
const FRIENDTECH_ADDRESS = "0xCF205808Ed36593aa40a44F10c7f7C2F67d4A4d4";

// Generate topic0 list from event signatures
const topic0_list = event_signatures.map((sig) => keccak256(toHex(sig)));
console.log(topic0_list);

// Initialize HypersyncClient
const client = HypersyncClient.new({
  url: "http://base.hypersync.xyz",
});

// Query setup
let query = {
  fromBlock: 0,
  logs: [
    {
      address: [FRIENDTECH_ADDRESS],
      topics: [topic0_list],
    },
  ],
  fieldSelection: {
    block: [
      "difficulty",
      "number",
      "timestamp",
      "hash",
      "extraData",
      "gasLimit",
      "gasUsed",
      "logsBloom",
      "miner",
      "mixHash",
      "nonce",
      "parentHash",
      "receiptsRoot",
      "sha3Uncles",
      "stateRoot",
      "totalDifficulty",
      "transactionsRoot",
      "size",
    ],
    log: [
      "block_number",
      "block_hash",
      "log_index",
      "transaction_index",
      "transaction_hash",
      "data",
      "address",
      "topic0",
      "topic1",
      "topic2",
      "topic3",
    ],
    transaction: ["from"],
  },
};

// Connect to SQLite database
const db = new sqlite3.Database("../.ponder/sqlite/ponder_sync.db");

// Function to insert data into the logs table
const insertLogsBatch = (logs) => {
  const insertQuery = `
    INSERT OR IGNORE INTO logs (
      address, blockHash, blockNumber, chainId, data, id, logIndex,
      topic0, topic1, topic2, topic3, transactionHash, transactionIndex, checkpoint
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  const stmt = db.prepare(insertQuery);

  db.serialize(() => {
    db.run("BEGIN TRANSACTION");
    logs.forEach((log) => {
      const { number: blockNumber, hash: blockHash } = log.block;
      const {
        logIndex,
        transactionIndex,
        transactionHash,
        data,
        address,
        topics,
      } = log.log;

      if (blockNumber === undefined || logIndex === undefined) {
        console.error("Missing block_number or log_index in log:", log);
        return;
      }

      const blockNumberStr = blockNumber.toString().padStart(79, "0");
      const logIndexHex = `0x${logIndex.toString(16)}`; // Convert logIndex to hexadecimal
      const id = `${blockHash}-${logIndexHex}`;
      const checkpoint = encodeCheckpoint({
        blockTimestamp: log.block.timestamp,
        chainId: 8453n,
        blockNumber: BigInt(blockNumber),
        transactionIndex: BigInt(transactionIndex),
        eventType: EVENT_TYPES.logs, // Assuming logs event type
        eventIndex: BigInt(logIndex),
      });

      stmt.run([
        address,
        blockHash,
        blockNumberStr,
        8453,
        data,
        id,
        logIndex,
        topics[0],
        topics[1] || "",
        topics[2] || "",
        topics[3] || "",
        transactionHash,
        transactionIndex,
        checkpoint,
      ]);
    });
    db.run("COMMIT", (err) => {
      if (err) {
        console.error("Error committing transaction:", err.message);
      }
    });
  });

  stmt.finalize();
};

// Main function
const main = async () => {
  let eventCount = 0;
  const startTime = performance.now();
  const batchSize = 1000;
  let eventBatch = [];

  // Initial non-parallelized request
  const res = await client.sendEventsReq(query);
  eventCount += res.events.length;
  //   console.log("Initial events:", res.events);
  eventBatch.push(...res.events);
  query.fromBlock = res.nextBlock;

  if (eventBatch.length >= batchSize) {
    insertLogsBatch(eventBatch);
    eventBatch = [];
  }

  // Streaming events in parallel
  const stream = await client.streamEvents(query, {
    retry: true,
    batchSize: 10000,
    concurrency: 12,
  });

  while (true) {
    const res = await stream.recv();

    // Quit if we reached the tip
    if (res === null) {
      console.log("reached the tip");
      break;
    }

    eventCount += res.events.length;
    eventBatch.push(...res.events);

    if (eventBatch.length >= batchSize) {
      insertLogsBatch(eventBatch);
      eventBatch = [];
    }

    const currentTime = performance.now();
    const seconds = (currentTime - startTime) / 1000;

    console.log(
      `scanned up to ${
        res.nextBlock
      } and got ${eventCount} events. ${seconds} seconds elapsed. Events per second: ${
        eventCount / seconds
      }`
    );
  }

  // Insert any remaining logs in the batch
  if (eventBatch.length > 0) {
    insertLogsBatch(eventBatch);
  }

  // Close the database connection
  db.close((err) => {
    if (err) {
      console.error("Error closing the database:", err.message);
    }
  });
};

main();
