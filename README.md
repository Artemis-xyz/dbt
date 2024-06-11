# Artemis DBT

![Type=Combination, Color=Mix](https://github.com/Artemis-xyz/dbt/assets/12548832/93c7c673-6cee-479c-ab9b-833e7dc9546b)

![githubWorkflowBadge]
![discordBadge]
![twitterBadge]

Welcome to the Artemis DBT repository!

## Table of Contents

- [Introduction](#introduction)
  - [What is this?](#what-is-this)
  - [Who is this for?](#who-is-this-for)
  - [wtf is DBT?](#wtf-is-dbt)
  - [How do I get help?](#how-do-i-get-help)
- [Envionment Setup](#environment-setup)
- [System Design](#system-design)
- [Adding a new asset](#adding-a-new-asset)
- [Adding a new metric](#adding-a-new-metric)

## Introduction

### What is this?

This is the home of all of the data-munging business logic that ultimately powers a variety of the data in the Artemis application suite, including the [Terminal](https://app.artemis.xyz/), [Sheets](https://www.artemis.xyz/sheets), and [Snowflake](https://www.artemis.xyz/datashare) integration.

### Who is this for?

If you're in this repository, you're likely one of the following:

- a **researcher** who is trying to better understand methodology
- a **protocol** that would like to self-list on Artemis
- a **data scientist** that would like to add new protocol or metric (and possibly be compensated for their contributions via a bounty)

### wtf is DBT?

DBT stands for data-build-tool and is an approach to building SQL models in a collaborative and iterative environment. It allows you to "modularize and centralize your analytics code" while mostly being vanilla SQL with some minor syntatic magic on top. This repository uses the **Snowflake SQL** syntax on top of DBT.

We use DBT to transform raw blockchain data (transactions, traces, and decoded event logs) into high-fidelity metrics for our users.
![image](https://github.com/Artemis-xyz/dbt/assets/12548832/a10bde59-46db-4e92-a230-825020e3ebe3)

For most SQL wizards, reading DBT models comes intutitvely, but below are some revelant resources to learn more:

- [What is DBT?](https://docs.getdbt.com/docs/introduction)
- [A Beginner's Guide to DBT](https://pttljessy.medium.com/a-beginners-guide-to-dbt-data-build-tool-part-1-introduction-9a147ada1eb9)

### How do I get help?

There are two ways to get help:

1. Pop into our [Discord](https://discord.com/invite/wMEA9k6n6T) and ask us anything in the Methodology channel (fastest)
2. Raise an issue on this Github.

## Environment Setup

1. Fork this repository (button towards the top right)
2. Write SQL model changes
3. Open a PR and view results of your changes directly in the Github Actions - more on this in [adding a new metric](#adding-a-new-metric).

## System Design

In terms of system design, Artemis manages a data pipeline that pipes raw data from a series of vendors, including [Flipside](https://flipsidecrypto.xyz/), [Goldsky](https://goldsky.com/), and [QuickNode](https://www.quicknode.com/chains) into our Snowflake data warehouse. This repository contains the business logic for turning raw data into `fact` tables that describe an individual protocol or collection of protocols.

<img width="1160" alt="Untitled (1) (1)" src="https://github.com/Artemis-xyz/dbt/assets/12548832/8178373b-ca67-4243-bfbb-73bbe475ff14">

Fact tables are then combined into `ez_asset_metrics` tables that are piped into the downstream applications. We use the [STAR schema model](https://en.wikipedia.org/wiki/Star_schema) to label our tables.

## Adding a new asset

**BEFORE** adding metrics for a protocol, you must create the asset first.

For example, let's say a user wants to add GEODNET fees to our [DePin dashboard.](https://app.artemis.xyz/sectors?tab=dePin). They must first add the [GEODNET](https://geodnet.com/) asset first by completing the following steps:

- [ ] Fork this repository
- [ ] Add GEODNET to the `databases.csv` file
- [ ] Request and merge a PR with this change

The Artemis team will then create the necessary permissions and warehouses in order for GEODNET to show up in the Terminal.

[Example PR](https://github.com/Artemis-xyz/dbt/pull/6)

## Adding a new metric

Once the asset exists, there are several ways to pull metrics for the protocol in question. Taking the GEODNET fees example, we will breakdown how to add this protocol's metrics by provider below.

GEODNET is a DePin protocol where [80% of fees are sent to the burn address](https://insidegnss.com/developing-a-truly-global-rtk-network/) and counted towards network revenue.

### Using Flipside's Warehouse

To calculate fees, we can write a query directly in Flipside's studio to count the token transfers towards the burn address, [found here](https://flipsidecrypto.xyz/0xnirmal/q/HAffpGdx4d63/geodnet-fees-revenue).

Productionizing this into the Artemis DBT schemas, fact tables are expected to have the following columns:

- `date` [DATETIME]
- `fees` [NUMBER] - this field name will change based on the metric you are pulling
- `chain` [STRING]
- `protocol` [STRING]

In this example, the chain that GEODNET publishes fees on is `polygon` and the protocol is `geodnet` - both of these fields should match the asset names in the `assets.csv` file.

Given our Flipside query is already in this format, we can mostly copy and paste this directly into a fact table in the correct directory for GEODNET: [`models/projects/geodnet/core/fact_geodnet_fees_revenue.sql`](https://github.com/Artemis-xyz/dbt/pull/8/files#diff-dc48c2558404951a14c60bc8d5a66093d6c8749c87fa2701ba4a59dd7a3f190eR1)

Note two important distinctions in this file:

1. Rather than using the the `polygon.core.ez_token_transfers` database and table, we use `polygon_flipside.core.ez_token_transfers`. This will tell the Artemis pipeline to pull from the Flipside Database.
2. The model has the following in the header, which tells DBT to materalize the results as a table: `{{ config(materialized="table") }}`

Now, we just need to create our `ez_geodnet_metrics` table in the right directory and query the results of the above fact table, shown here: [`models/projects/geodnet/core/ez_geodnet_metrics.sql`](https://github.com/Artemis-xyz/dbt/pull/8/files#diff-f91add043544db867d0d97e874675ef6b550717f8ba8a47473975b5a0622e711R11)

We are done! Open up a PR targeting this repository and check to make sure the code compiles in the Github actions.

You can see a demo output of your new query by clicking on the "Details" of the `Show Changed Models` CI step.
<img width="840" alt="image" src="https://github.com/Artemis-xyz/dbt/assets/12548832/c7ac9f68-21c5-441f-8da7-c5fd3d378bd2">

<img width="1055" alt="Screenshot 2024-04-04 at 12 01 52 PM" src="https://github.com/Artemis-xyz/dbt/assets/12548832/135d3258-4122-430d-af22-ce06de9db3f3">

### Using QuickNode's RPCs

Out of scope for right now. Will be added on future iterations.

### Using Goldsky-ingested Raw Data

Out of scope for right now. Will be added on future iterations.

[discordBadge]: https://img.shields.io/discord/1042835101056258098?label=discord&logo=discord&logoColor=white
[githubWorkflowBadge]: https://github.com/Artemis-xyz/dbt/actions/workflows/compile.yml/badge.svg
[twitterBadge]: https://img.shields.io/twitter/follow/artemis__xyz?link=https%3A%2F%2Ftwitter.com%2Fartemis__xyz
