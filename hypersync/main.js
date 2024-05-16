import { keccak256, toHex } from "viem";
import { HypersyncClient } from "@envio-dev/hypersync-client";
import { Worker } from "worker_threads";

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

// Create a worker for database operations
const dbWorker = new Worker("./dbWorker.js");

// Function to send messages to the worker
const sendToWorker = (message) => {
  dbWorker.postMessage(message);
};

// Function to get the largest block number from the blocks table
const getLargestBlockNumber = () => {
  return new Promise((resolve, reject) => {
    dbWorker.once("message", resolve);
    dbWorker.postMessage({ type: "getLargestBlockNumber" });
  });
};

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
    transaction: [
      "block_hash",
      "block_number",
      "chain_id",
      "contract_address",
      "cumulative_gas_used",
      "effective_gas_price",
      "from",
      "gas",
      "gas_price",
      "gas_used",
      "hash",
      "input",
      "kind",
      "logs_bloom",
      "max_fee_per_gas",
      "max_priority_fee_per_gas",
      "nonce",
      "r",
      "root",
      "s",
      "status",
      "to",
      "transaction_index",
      "v",
      "value",
    ],
  },
};

// Function to extract unique blocks and transactions from events
const extractUniqueBlocksAndTransactions = (events) => {
  const blockMap = new Map();
  const transactionMap = new Map();

  events.forEach((event) => {
    const block = event.block;
    const transaction = event.transaction;

    if (!blockMap.has(block.hash)) {
      blockMap.set(block.hash, block);
    }

    if (!transactionMap.has(transaction.hash)) {
      transactionMap.set(transaction.hash, transaction);
    }
  });

  return {
    blocks: Array.from(blockMap.values()),
    transactions: Array.from(transactionMap.values()),
  };
};

// Main function
const main = async () => {
  let eventCount = 0;
  const startTime = performance.now();
  const batchSize = 1000; // Adjusted batch size
  let eventBatch = [];
  let blockBatch = [];
  let transactionBatch = [];
  const memoryCheckInterval = 30_000; // Check memory every 30 seconds

  // Get the largest block number from the blocks table
  const largestBlockNumber = await getLargestBlockNumber();
  query.fromBlock = largestBlockNumber + 1;

  // Initial non-parallelized request
  const res = await client.sendEventsReq(query);
  eventCount += res.events.length;
  console.log("Initial events:", res.events);
  eventBatch.push(...res.events);
  const { blocks, transactions } = extractUniqueBlocksAndTransactions(
    res.events
  );
  blockBatch.push(...blocks);
  transactionBatch.push(...transactions);
  query.fromBlock = res.nextBlock;

  if (eventBatch.length >= batchSize) {
    sendToWorker({ type: "insertLogsBatch", data: eventBatch });
    sendToWorker({
      type: "insertLogFilterInterval",
      startBlock: eventBatch[0].block.number,
      endBlock: eventBatch[eventBatch.length - 1].block.number,
    });
    eventBatch = [];
  }

  if (blockBatch.length >= batchSize) {
    sendToWorker({ type: "insertBlocksBatch", data: blockBatch });
    blockBatch = [];
  }

  if (transactionBatch.length >= batchSize) {
    sendToWorker({ type: "insertTransactionsBatch", data: transactionBatch });
    transactionBatch = [];
  }

  // Streaming events in parallel
  const stream = await client.streamEvents(query, {
    retry: true,
    batchSize: 1000, // Adjusted batch size for streaming
    concurrency: 10,
  });

  const memoryLogger = setInterval(() => {
    if (global.gc) {
      global.gc();
    }
    const memoryUsage = process.memoryUsage();
    console.log("Memory Usage:", {
      rss: memoryUsage.rss,
      heapTotal: memoryUsage.heapTotal,
      heapUsed: memoryUsage.heapUsed,
      external: memoryUsage.external,
      arrayBuffers: memoryUsage.arrayBuffers,
    });
  }, memoryCheckInterval);

  while (true) {
    const res = await stream.recv();

    // Quit if we reached the tip
    if (res === null) {
      console.log("reached the tip");
      clearInterval(memoryLogger);
      break;
    }

    eventCount += res.events.length;
    eventBatch.push(...res.events);
    const { blocks, transactions } = extractUniqueBlocksAndTransactions(
      res.events
    );
    blockBatch.push(...blocks);
    transactionBatch.push(...transactions);

    if (eventBatch.length >= batchSize) {
      sendToWorker({ type: "insertLogsBatch", data: eventBatch });
      sendToWorker({
        type: "insertLogFilterInterval",
        startBlock: eventBatch[0].block.number,
        endBlock: eventBatch[eventBatch.length - 1].block.number,
      });
      eventBatch = [];
    }

    if (blockBatch.length >= batchSize) {
      sendToWorker({ type: "insertBlocksBatch", data: blockBatch });
      blockBatch = [];
    }

    if (transactionBatch.length >= batchSize) {
      sendToWorker({ type: "insertTransactionsBatch", data: transactionBatch });
      transactionBatch = [];
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

    // Yield to event loop to allow garbage collection
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  // Insert any remaining logs, blocks, and transactions in the batch
  if (eventBatch.length > 0) {
    sendToWorker({ type: "insertLogsBatch", data: eventBatch });
    sendToWorker({
      type: "insertLogFilterInterval",
      startBlock: eventBatch[0].block.number,
      endBlock: eventBatch[eventBatch.length - 1].block.number,
    });
  }

  if (blockBatch.length > 0) {
    sendToWorker({ type: "insertBlocksBatch", data: blockBatch });
  }

  if (transactionBatch.length > 0) {
    sendToWorker({ type: "insertTransactionsBatch", data: transactionBatch });
  }

  // Close the database connection
  dbWorker.postMessage({ type: "close" });
};

main();
