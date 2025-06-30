# Crypto Tax Reporting Suite

This suite of scripts started out as a comprehensive crypto tax reporting for TON (The Open Network) wallets only, including transaction processing, FMV calculations, and CRA-compliant report generation. Now it supports more major blockchains, provided all trasactions are saved as csv.

## Key Features

- **Transaction Processing**: Fetches and processes all wallet transactions for TON
- **FMV Integration**: Uses historical exchange rates from CoinGecko free API capped at 30 calls/ minute and 10K/month (at time of publish)
- **CRA Compliance**: Generates tax reports meeting Canadian Revenue Agency requirements
- **Automated Workflow**: Complete processing from raw transactions to final reports
- **Error Handling**: Robust logging and validation throughout the process


## Setup & Workflow

### Prerequisites

- Ruby 3.4.4
- Bundler gem installed
- CoinGecko API key (for FMV data)

### Getting Started

1. Clone the repository:
    ```sh
    git clone https://github.com/samzhab/tonapi_wallet_explorer.git
    ```
2. Crete a Gemset:
    ```sh
    rvm 3.4.4@tonapi --create
    ```

3. Install dependencies:
    ```sh
    bundle install
    ```

4. Create necessary directories
    ```sh
    mkdir CSV_Files
    ```
### Workflow

5. Gather all blockchain transactions for each wallet as csv and add chain name as prefix to file such as ```arbi_export-0xdEe1f25Cd66Db7cf4d7335A95A14324DFF91a588.csv``` in ```CSV_Files``` for Arbitrium for instance. To gather all transactions for TON blockchain wallets, run the script with your wallet addresses - The script will generate a CSV file for specified wallets on TON.

    ```sh
    ruby ton_transactions_explorer.rb
    ```

    then get FMV for each transaction using CoinGecko API
    ```sh
    ruby historical_fmv_requester.rb
    ```

    then check for missing FMV for each transaction (**optional**)
    ```sh
    ruby missing_historial_fmv_checker.rb
    ```

    then do another FMV fetch for missing FMV
    ```sh
    ruby find_missing_historical_fmv.rb
    ```

    then update missing historical FMV
    ```sh
    ruby update_missing_historical_fmv.rb
    ```

    finally, run crypto tax generator
    ```sh
    ruby cra_cryptotax_generator.rb
    ```

## Key features implemented:

### 1. **Multi-chain support**:
- Automatic detection of **12 major blockchains** (TON, ETH, SOL, BASE, BSC, LINEA, OPBNB, TRX, ARB, OP, SCROLL, SONIC, ZKSYNC)
- Chain-specific pattern matching for addresses and filenames
- Separate cache files for each blockchain

### 2. **Smart caching system**:
- Maintains two cache files per chain:
    * **{chain}_historical_fmv.yaml** - Successfully fetched rates
    * **{chain}_missing_historical_fmv.yaml** - Dates with failed fetches
- **SHA256-based file processing** tracking to avoid reprocessing
- Thread-safe parallel processing of multiple files

3. **Enhanced CoinGecko API Integration**:
Here's a more concise README update focusing on key functionality rather than implementation details:

### 3. **CoinGecko API Integration**

#### **Core Features**
- Automatic historical price fetching (max 365 days)
- Smart date grouping to minimize duplicate API calls
- Case-insensitive currency support (CAD/USD/etc)

#### **Rate & Error Handling**
- Built-in rate limiting compliant with API requirements
- Intelligent retry system for failed requests
- Comprehensive handling of all API error responses
- Never skips price pairs - persistent retry attempts

#### **Reliability**
- Resilient to network interruptions
- Automatic recovery from temporary failures
- Detailed logging of all fetch attempts
- Caching system to prevent redundant calls

#### **Key Improvements**
- Fixed "undefined retries" bug in error handling
- Enhanced 429 rate limit recovery
- Added support for all CoinGecko API status codes
- Improved timeout and network failure handling

### 4. **Enhanced reporting**:
   - Detailed console output during processing
   - Final summary of cached rates vs missing dates
   - Clear progress tracking for large batches

### 5. **Optimization features**:
   - Parallel processing (3 threads)
   - Date batching to minimize API calls
   - Skip logic for already processed dates/files
   - Automatic handling of old dates (>365 days)

The implementation includes robust error handling and automatically creates necessary directory structure. Each blockchain maintains separate cache files for better organization and maintenance.

**Example usage:**
```sh
$ ruby fmv_processor.rb
```

Final output shows cache statistics for easy monitoring of data
coverage across all supported blockchains.

    Processing 4 files...
    Fetching 12 dates for eth...
    Added 8 new rates to eth_historical_fmv.yaml
    Added 4 missing dates to eth_missing_historical_fmv.yaml
    Fetching 5 dates for trx...
    Added 3 new rates to trx_historical_fmv.yaml
    Added 2 missing dates to trx_missing_historical_fmv.yaml

    Processing complete. Summary of cache files:
    ETH: 125 rates | 4 missing dates
    TRX: 87 rates | 2 missing dates


## License:
 This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

 ![CC BY-SA 4.0](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)

 Attribution: This project is published by SamaelLabs (AI Powered), 2025.

 You are free to:
 - Share — copy and redistribute the material in any medium or format
 - Adapt — remix, transform, and build upon the material for any purpose, even commercially.
 Under the following terms:
 - Attribution — You must give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 - ShareAlike — If you remix, transform, or build upon the material, you must distribute your contributions under the same license as the original.

 No additional restrictions — You may not apply legal terms or technological measures that legally restrict others from doing anything the license permits.

 Notices:
 You do not have to comply with the license for elements of the material in the public domain or where your use is permitted by an applicable exception or limitation.

 No warranties are given. The license may not give you all of the permissions necessary for your intended use. For example, other rights such as publicity, privacy, or moral rights may limit how you use the material.
