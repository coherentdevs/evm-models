# Ethereum DBT Models and Tables

Welcome to our Ethereum DBT Models and Tables repository! We provide a set of structured and modular DBT models for decoding and
analyzing Ethereum transaction data. Our models are designed to be efficient, easily extensible, and accessible for developers.

## Overview

Our Ethereum DBT Models and Tables provide a solid foundation for developers to build and analyze Ethereum transaction data. We focus on two base models, raw and decoded, which can be extended with custom DBT models to fit your specific use case.

## Getting Started

To get started, clone this repository and follow the instructions in the `README.md` file. Configure your `profiles.yml` file with the appropriate Snowflake credentials and warehouse settings. Once you have the repository set up, you can start running DBT commands to build and analyze the models.

## Base Models

Our base models include:

- `decoded_transactions`: Contains decoded Ethereum transaction data based on the available ABIs.
- `decoded_traces`: Contains decoded Ethereum traces data based on the available ABIs.
- `decoded_logs`: Contains decoded Ethereum logs data based on the available ABIs.
- `decoded_blocks`: Contains human readable and queriable Ethereum block data.

These models can be extended with custom DBT models to fit your specific needs. We have the same base models for traces, blocks, and logs as well

## Custom Models

Creating custom DBT models allows you to build off of our base models and tailor the analysis to your specific use case. By extending our base models, you can efficiently analyze and extract insights from Ethereum transaction data.

To create custom DBT models, follow these steps:

1. Create a new DBT model file (e.g., uniswap_transactions.sql) under the `models` directory.
2. Use the base models as a starting point.
3. Add your custom logic and filters.
4. Run the custom model using DBT.

## Commands

Here are some useful DBT commands to get you started:

- `dbt debug`: Test your DBT setup and Snowflake connection.
- `dbt run`: Runs the models in a project
- `dbt build`: Build and test all selected resources in a project

## Additional Resources

For more in-depth technical documentation, please refer to the following links:

- [Eth Mainnet Guide](https://docs.datadao.dev/warehouse-data-shares/ethereum-mainnet/guide)
- [DBT Models](https://docs.datadao.dev/warehouse-data-shares/ethereum-mainnet/dbt-models)
- [DBT Installation](https://docs.getdbt.com/docs/core/installation)
- [DBT Commands](https://docs.getdbt.com/reference/dbt-commands)
