import { parentPort } from "worker_threads";
import Database from "better-sqlite3";
import { encodeCheckpoint, EVENT_TYPES } from "./checkpoint.js"; // Adjusted to import from the new JavaScript file

// Connect to SQLite database using better-sqlite3 with a busy timeout
const db = new Database("../.ponder/sqlite/ponder_sync.db", {
  busyTimeout: 60000,
}); // 60 seconds

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

// Function to insert data into the logs table with retry logic
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

  try {
    insertMany(logs);
  } catch (error) {
    if (error.code === "SQLITE_BUSY") {
      console.error("Database is busy, retrying...");
      setTimeout(() => insertMany(logs), 100); // Retry after 100ms
    } else {
      throw error;
    }
  }
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

  try {
    insertMany(blocks);
  } catch (error) {
    if (error.code === "SQLITE_BUSY") {
      console.error("Database is busy, retrying...");
      setTimeout(() => insertMany(blocks), 100); // Retry after 100ms
    } else {
      throw error;
    }
  }
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

  try {
    insertMany(transactions);
  } catch (error) {
    if (error.code === "SQLITE_BUSY") {
      console.error("Database is busy, retrying...");
      setTimeout(() => insertMany(transactions), 100); // Retry after 100ms
    } else {
      throw error;
    }
  }
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

// Function to get the largest block number from the blocks table
const getLargestBlockNumber = () => {
  const query = `SELECT MAX(number) as maxBlockNumber FROM blocks`;
  const row = db.prepare(query).get();
  if (row && row.maxBlockNumber) {
    return parseInt(row.maxBlockNumber);
  }
  return 0; // Return 0 if no blocks are found
};

// Handle messages from the parent thread
parentPort.on("message", async (message) => {
  try {
    switch (message.type) {
      case "insertLogsBatch":
        insertLogsBatch(message.data);
        break;
      case "insertBlocksBatch":
        insertBlocksBatch(message.data);
        break;
      case "insertTransactionsBatch":
        insertTransactionsBatch(message.data);
        break;
      case "insertLogFilterInterval":
        insertLogFilterInterval(message.startBlock, message.endBlock);
        break;
      case "getLargestBlockNumber":
        parentPort.postMessage(await getLargestBlockNumber());
        break;
      case "close":
        db.close();
        break;
      default:
        console.error(`Unknown message type: ${message.type}`);
    }
  } catch (error) {
    console.error(`Worker error: ${error}`);
  }
});
