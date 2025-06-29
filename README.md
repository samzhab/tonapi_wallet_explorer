# TON Crypto Tax Reporting Suite

This suite of scripts helps with comprehensive crypto tax reporting for TON (The Open Network) wallets, including transaction processing, FMV calculations, and CRA-compliant report generation.

## Key Features

- **Transaction Processing**: Fetches and processes all wallet transactions
- **FMV Integration**: Uses historical exchange rates from CoinGecko API
- **CRA Compliance**: Generates tax reports meeting Canadian Revenue Agency requirements
- **Automated Workflow**: Complete processing from raw transactions to final reports
- **Error Handling**: Robust logging and validation throughout the process

## Setup Instructions

### Prerequisites

- Ruby 3.4.4
- Bundler gem installed
- CoinGecko API key (for FMV data)

### Steps

## Getting Started

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

5. Run the script - The script will generate a CSV file for specified wallet.

    ```sh
    ruby ton_transactions_explorer.rb
    ```

    then get FMV for each transaction using CoinGecko API
    ```sh
    ruby historical_fmv_requester.rb
    ```

    then check for missing FMV for each transaction
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

 ## License
 This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

 ![CC BY-SA 4.0](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)

 Attribution: This project is published by Samael (AI Powered), 2024.

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
