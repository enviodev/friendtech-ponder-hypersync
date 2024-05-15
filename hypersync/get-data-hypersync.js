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
      "extra_data",
      "gas_limit",
      "gas_used",
      "logs_bloom",
      "miner",
      "mix_hash",
      "nonce",
      "parent_hash",
      "receipts_root",
      "sha3_uncles",
      "state_root",
      "total_difficulty",
      "transactions_root",
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

const convertHexToPaddedDecimal = (hexValue, totalLength) => {
  try {
    // Handle '0x00' explicitly
    if (hexValue === "0x00") {
      return "0".padStart(totalLength, "0");
    }
    // Convert hex to BigInt, then to string, then pad with leading zeros
    const decimalValue = BigInt(hexValue).toString();
    return decimalValue.padStart(totalLength, "0");
  } catch (error) {
    console.error(`Error converting hex value ${hexValue}:`, error);
    return "0".padStart(totalLength, "0"); // Fallback to default value
  }
};

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

// Function to insert data into the blocks table
const insertBlocksBatch = (blocks) => {
  const insertQuery = `
    INSERT OR IGNORE INTO blocks (
      difficulty, extraData, gasLimit, gasUsed, hash, logsBloom, miner,
      number, parentHash, receiptsRoot, size, stateRoot, timestamp, transactionsRoot,
      chainId, checkpoint, baseFeePerGas, mixHash, nonce, sha3Uncles, totalDifficulty
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  const stmt = db.prepare(insertQuery);

  db.serialize(() => {
    db.run("BEGIN TRANSACTION");
    blocks.forEach((block) => {
      const {
        difficulty = "0x00",
        extraData = "",
        gasLimit = "0x00",
        gasUsed = "0x00",
        hash,
        logsBloom = "".padEnd(514, "0"),
        miner = "",
        nonce = "".padStart(18, "0"),
        number,
        parentHash = "",
        receiptsRoot = "",
        sha3Uncles = "",
        size = "0x00",
        stateRoot = "",
        timestamp,
        totalDifficulty = "0x00",
        transactionsRoot = "",
        baseFeePerGas = "0x00",
        mixHash = "",
      } = block;
      if (
        number === undefined ||
        timestamp === undefined ||
        hash === undefined
      ) {
        console.error("Missing number, timestamp, or hash in block:", block);
        return;
      }

      const numberStr = number.toString().padStart(79, "0");
      const timestampStr = timestamp.toString().padStart(79, "0");
      const difficultyStr = convertHexToPaddedDecimal(difficulty, 79); // Convert difficulty to padded decimal
      const gasLimitStr = convertHexToPaddedDecimal(gasLimit, 79); // Convert gasLimit to padded decimal
      const gasUsedStr = convertHexToPaddedDecimal(gasUsed, 79); // Convert gasUsed to padded decimal
      const sizeStr = convertHexToPaddedDecimal(size, 79); // Convert size to padded decimal
      const totalDifficultyStr = convertHexToPaddedDecimal(totalDifficulty, 79); // Convert totalDifficulty to padded decimal
      const baseFeePerGasStr = convertHexToPaddedDecimal(baseFeePerGas, 79); // Convert baseFeePerGas to padded decimal
      const checkpointValue = encodeCheckpoint({
        blockTimestamp: timestamp,
        chainId: 8453n,
        blockNumber: BigInt(number),
        transactionIndex: 9999999999999999n, // Not relevant for blocks
        eventType: EVENT_TYPES.blocks, // Assuming blocks event type
        eventIndex: 0n, // Not relevant for blocks
      });

      stmt.run([
        difficultyStr,
        extraData,
        gasLimitStr,
        gasUsedStr,
        hash,
        logsBloom,
        miner,
        numberStr,
        parentHash,
        receiptsRoot,
        sizeStr,
        stateRoot,
        timestampStr,
        transactionsRoot,
        8453,
        checkpointValue,
        baseFeePerGasStr,
        mixHash,
        nonce,
        sha3Uncles,
        totalDifficultyStr,
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

// Function to extract unique blocks from events
const extractUniqueBlocks = (events) => {
  const blockMap = new Map();

  events.forEach((event) => {
    const block = event.block;
    if (!blockMap.has(block.hash)) {
      blockMap.set(block.hash, block);
    }
  });

  return Array.from(blockMap.values());
};

// Main function
const main = async () => {
  let eventCount = 0;
  const startTime = performance.now();
  const batchSize = 1000;
  let eventBatch = [];
  let blockBatch = [];

  // Initial non-parallelized request
  const res = await client.sendEventsReq(query);
  eventCount += res.events.length;
  console.log("Initial events:", res.events);
  eventBatch.push(...res.events);
  blockBatch.push(...extractUniqueBlocks(res.events));
  query.fromBlock = res.nextBlock;

  if (eventBatch.length >= batchSize) {
    insertLogsBatch(eventBatch);
    eventBatch = [];
  }

  if (blockBatch.length >= batchSize) {
    insertBlocksBatch(blockBatch);
    blockBatch = [];
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
    blockBatch.push(...extractUniqueBlocks(res.events));

    if (eventBatch.length >= batchSize) {
      insertLogsBatch(eventBatch);
      eventBatch = [];
    }

    if (blockBatch.length >= batchSize) {
      insertBlocksBatch(blockBatch);
      blockBatch = [];
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

  // Insert any remaining logs and blocks in the batch
  if (eventBatch.length > 0) {
    insertLogsBatch(eventBatch);
  }

  if (blockBatch.length > 0) {
    insertBlocksBatch(blockBatch);
  }

  // Close the database connection
  db.close((err) => {
    if (err) {
      console.error("Error closing the database:", err.message);
    }
  });
};

main();
