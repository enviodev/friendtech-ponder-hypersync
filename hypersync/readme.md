Run this process to rapidly sync data to the ponder_sync.db

First run pnpm ponder dev to create the sqlite db and stop this process sometime after its been created (maybe 10 seconds)

Second, cd in hypersync folder and run node --expose-gc get-data-hypersync.js or node get-data-hypersync.js
This will fill the ponder_sync.db with all the required raw data.
