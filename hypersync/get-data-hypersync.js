import { keccak256, toHex } from "viem";
import { HypersyncClient } from "@envio-dev/hypersync-client";
import Database from "better-sqlite3";
import { encodeCheckpoint, EVENT_TYPES } from "./checkpoint.js"; // Adjusted to import from the new JavaScript file

// Event signatures and address
const event_signatures = [
  "Trade(address,address,bool,uint256,uint256,uint256,uint256,uint256)",
];
const FRIENDTECH_ADDRESS = "0xCF205808Ed36593aa40a44F10c7f7C2F67d4A4d4";

// Connect to SQLite database using better-sqlite3
const db = new Database("../.ponder/sqlite/ponder_sync.db");

// Function to get the largest block number from the blocks table
const getLargestBlockNumber = () => {
  const row = db
    .prepare("SELECT MAX(number) as maxBlockNumber FROM blocks")
    .get();
  return row.maxBlockNumber ? parseInt(row.maxBlockNumber, 10) : 0;
};
// Get the largest block number from the blocks table
const largestBlockNumber = getLargestBlockNumber();
console.log(largestBlockNumber);

// Generate topic0 list from event signatures
const topic0_list = event_signatures.map((sig) => keccak256(toHex(sig)));
console.log(topic0_list);

// Initialize HypersyncClient
const client = HypersyncClient.new({
  url: "http://base.hypersync.xyz",
});

// Query setup
let query = {
  fromBlock: largestBlockNumber + 1,
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

// Function to convert hex to padded decimal
const convertHexToPaddedDecimal = (hexValue, totalLength) => {
  try {
    if (hexValue === "0x00") {
      return "0".padStart(totalLength, "0");
    }
    const decimalValue = BigInt(hexValue).toString();
    return decimalValue.padStart(totalLength, "0");
  } catch (error) {
    console.error(`Error converting hex value ${hexValue}:`, error);
    return "0".padStart(totalLength, "0");
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
  const insertMany = db.transaction((logs) => {
    for (const log of logs) {
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
        continue;
      }

      const blockNumberStr = blockNumber.toString().padStart(79, "0");
      const logIndexHex = `0x${logIndex.toString(16)}`;
      const id = `${blockHash}-${logIndexHex}`;
      const checkpoint = encodeCheckpoint({
        blockTimestamp: log.block.timestamp,
        chainId: 8453n,
        blockNumber: BigInt(blockNumber),
        transactionIndex: BigInt(transactionIndex),
        eventType: EVENT_TYPES.logs,
        eventIndex: BigInt(logIndex),
      });

      stmt.run(
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
        checkpoint
      );
    }
  });

  insertMany(logs);
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
  const insertMany = db.transaction((blocks) => {
    for (const block of blocks) {
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
        continue;
      }

      const numberStr = number.toString().padStart(79, "0");
      const timestampStr = timestamp.toString().padStart(79, "0");
      const difficultyStr = convertHexToPaddedDecimal(difficulty, 79);
      const gasLimitStr = convertHexToPaddedDecimal(gasLimit, 79);
      const gasUsedStr = convertHexToPaddedDecimal(gasUsed, 79);
      const sizeStr = convertHexToPaddedDecimal(size, 79);
      const totalDifficultyStr = convertHexToPaddedDecimal(totalDifficulty, 79);
      const baseFeePerGasStr = convertHexToPaddedDecimal(baseFeePerGas, 79);
      const checkpointValue = encodeCheckpoint({
        blockTimestamp: timestamp,
        chainId: 8453n,
        blockNumber: BigInt(number),
        transactionIndex: 9999999999999999n,
        eventType: EVENT_TYPES.blocks,
        eventIndex: 0n,
      });

      stmt.run(
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
        totalDifficultyStr
      );
    }
  });

  insertMany(blocks);
};

// Function to insert data into the transactions table
const insertTransactionsBatch = (transactions) => {
  const insertQuery = `
    INSERT OR IGNORE INTO transactions (
      accessList, blockHash, blockNumber, chainId, "from", gas, gasPrice, hash,
      input, maxFeePerGas, maxPriorityFeePerGas, nonce, "to", transactionIndex,
      type, value, r, s, v
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  const stmt = db.prepare(insertQuery);
  const insertMany = db.transaction((transactions) => {
    for (const tx of transactions) {
      const {
        accessList = "[]",
        blockHash,
        blockNumber,
        from,
        gas = "0x00",
        gasPrice = "0x00",
        hash,
        input = "",
        maxFeePerGas = "0x00",
        maxPriorityFeePerGas = "0x00",
        nonce,
        to,
        transactionIndex,
        type = "0x2",
        value = "0x00",
        r,
        s,
        v,
      } = tx;

      const blockNumberStr = blockNumber.toString().padStart(79, "0");
      const gasStr = convertHexToPaddedDecimal(gas, 79);
      const gasPriceStr = convertHexToPaddedDecimal(gasPrice, 79);
      const maxFeePerGasStr = convertHexToPaddedDecimal(maxFeePerGas, 79);
      const maxPriorityFeePerGasStr = convertHexToPaddedDecimal(
        maxPriorityFeePerGas,
        79
      );
      const valueStr = convertHexToPaddedDecimal(value, 79);
      const vStr = convertHexToPaddedDecimal(v, 79);
      const nonceDecimal = parseInt(nonce, 16);

      stmt.run(
        accessList,
        blockHash,
        blockNumberStr,
        8453,
        from,
        gasStr,
        gasPriceStr,
        hash,
        input,
        maxFeePerGasStr,
        maxPriorityFeePerGasStr,
        nonceDecimal,
        to,
        transactionIndex,
        type,
        valueStr,
        r,
        s,
        vStr
      );
    }
  });

  insertMany(transactions);
};

// Function to insert data into the logFilterIntervals table
const insertLogFilterInterval = (startBlock, endBlock) => {
  const insertQuery = `
    INSERT INTO logFilterIntervals (
      logFilterId, startBlock, endBlock
    ) VALUES (?, ?, ?)
  `;

  const logFilterId =
    "8453_0xcf205808ed36593aa40a44f10c7f7c2f67d4a4d4_0x2c76e7a47fd53e2854856ac3f0a5f3ee40d15cfaa82266357ea9779c486ab9c3_null_null_null_0";

  const startBlockStr = startBlock.toString().padStart(79, "0");
  const endBlockStr = endBlock.toString().padStart(79, "0");

  const stmt = db.prepare(insertQuery);
  stmt.run(logFilterId, startBlockStr, endBlockStr);
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

  // Initial non-parallelized request
  const res = await client.sendEventsReq(query);
  eventCount += res.events.length;
  // console.log("Initial events:", res.events);
  eventBatch.push(...res.events);
  const { blocks, transactions } = extractUniqueBlocksAndTransactions(
    res.events
  );
  blockBatch.push(...blocks);
  transactionBatch.push(...transactions);
  query.fromBlock = res.nextBlock;

  if (eventBatch.length >= batchSize) {
    insertLogsBatch(eventBatch);
    insertLogFilterInterval(
      eventBatch[0].block.number,
      eventBatch[eventBatch.length - 1].block.number
    );
    eventBatch = [];
  }

  if (blockBatch.length >= batchSize) {
    insertBlocksBatch(blockBatch);
    blockBatch = [];
  }

  if (transactionBatch.length >= batchSize) {
    insertTransactionsBatch(transactionBatch);
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
      insertLogsBatch(eventBatch);
      insertLogFilterInterval(
        eventBatch[0].block.number,
        eventBatch[eventBatch.length - 1].block.number
      );
      eventBatch = [];
    }

    if (blockBatch.length >= batchSize) {
      insertBlocksBatch(blockBatch);
      blockBatch = [];
    }

    if (transactionBatch.length >= batchSize) {
      insertTransactionsBatch(transactionBatch);
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
    insertLogsBatch(eventBatch);
    insertLogFilterInterval(
      eventBatch[0].block.number,
      eventBatch[eventBatch.length - 1].block.number
    );
  }

  if (blockBatch.length > 0) {
    insertBlocksBatch(blockBatch);
  }

  if (transactionBatch.length > 0) {
    insertTransactionsBatch(transactionBatch);
  }

  // Close the database connection
  db.close((err) => {
    if (err) {
      console.error("Error closing the database:", err.message);
    }
  });
};

main();
