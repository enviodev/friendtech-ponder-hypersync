# Rapid Sync to `ponder_sync.db`

This guide provides instructions to rapidly sync data to the `ponder_sync.db` using two possible scripts. Follow the steps below to set up and run the scripts effectively.

## Prerequisites

- **Node.js** (ensure you have version 12.x or later)
- **pnpm** (install via `npm install -g pnpm`)

## Steps to Sync Data

### 1. Initial Setup

1. **Create the SQLite Database**:
   - Run the following command to initialize the `ponder_sync.db`:
     ```sh
     pnpm ponder dev
     ```
   - Stop the process after approximately 10 seconds to allow the database to be created.

### 2. Sync Data Using Scripts

You have two options to sync data to `ponder_sync.db`:

#### Option 1: One-Script Wonder (`get-data-hypersync.js`)

This script contains all the logic in one file for easier understanding and execution.

1. **Navigate to the `hypersync` Folder**:

   ```sh
   cd hypersync
   ```

2. **Run the Script**:
   ```sh
   node --expose-gc get-data-hypersync.js
   ```
   or
   ```sh
   node get-data-hypersync.js
   ```

This will fill the `ponder_sync.db` with the required raw data by making requests to Hypersync and writing the results to the SQLite database.

#### Option 2: Separate Database Writing Process (`main.js`)

This script offloads database writing to a separate process to prevent it from becoming a bottleneck.

1. **Navigate to the `hypersync` Folder**:

   ```sh
   cd hypersync
   ```

2. **Run the Script**:
   ```sh
   node main.js
   ```

This script will fetch data and delegate the database writing workload to a separate process. Note that once all data is fetched, the database writing process will continue for a while, inserting data as this is the current bottleneck.

### 3. Utilize Historical Sync Information

Once you have filled the `ponder_sync.db` with some data using either of the above scripts, you can run the following command to utilize the historical sync information for a quicker sync:

```sh
pnpm ponder dev
```

## Summary

- **`get-data-hypersync.js`**: Single script for fetching and writing data.
- **`main.js`**: Separates database writing into a different process for better performance.

Choose the script that best suits your needs and follow the steps to quickly and efficiently populate your `ponder_sync.db`.

---

### Additional Notes

- Ensure you have `pnpm` installed globally.
- The `node --expose-gc` flag is optional but can help with garbage collection during large data sync operations.
- Monitor the `ponder_sync.db` file size and performance to ensure optimal operation.

---

By following these instructions, you will be able to rapidly sync data to `ponder_sync.db` and make use of the historical sync information for quicker subsequent syncs.

---
