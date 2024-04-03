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
- a **data analyst** that would like to add new protocol or metric (and possibly be compensated for their contributions via a bounty)

### wtf is DBT?
DBT stands for data-build-tool and is an approach to building SQL models in a collaborative and iterative environment. It allows you to "modularize and centralize your analytics code" while mostly being vanilla SQL with some minor syntatic magic on top. This repository uses the **Snowflake SQL** syntax on top of DBT.

We use DBT to transform raw blockchain data (transactions, traces, and decoded event logs) into high-fidelity metrics for our users. 
![image](https://github.com/Artemis-xyz/dbt/assets/12548832/a10bde59-46db-4e92-a230-825020e3ebe3)


For most SQL wizards, reading DBT models comes intutitvely, but below are some revelant resources to learn more:
- [What is DBT?](https://docs.getdbt.com/docs/introduction)
- [A Beginner's Guide to DBT](https://pttljessy.medium.com/a-beginners-guide-to-dbt-data-build-tool-part-1-introduction-9a147ada1eb9)

### How do I get help?
There are two ways to get help:
1) Pop into our [Discord](https://discord.com/invite/wMEA9k6n6T) and ask us anything in the Methodology channel (fastest)
2) Raise an issue on this Github.

## Environment Setup
1) Fork this repository (button towards the top right) 
2) Write SQL model changes
3) Open a PR and view results of your changes directly in the Github Actions - more on this in [adding a new metric](#adding-a-new-metric).

## System Design
<img width="1160" alt="Untitled (1) (1)" src="https://github.com/Artemis-xyz/dbt/assets/12548832/8178373b-ca67-4243-bfbb-73bbe475ff14">





## Adding a new asset

## Adding a new metric

### Using Flipside's Warehouse

### Using QuickNode's RPCs
Out of scope for right now. Will be added on future iterations.

### Using Goldsky-ingested Raw Data 
Out of scope for right now. Will be added on future iterations.


[discordBadge]: https://img.shields.io/discord/1042835101056258098?label=discord&logo=discord&logoColor=white
[githubWorkflowBadge]: https://github.com/Artemis-xyz/dbt/actions/workflows/compile.yml/badge.svg
[twitterBadge]: https://img.shields.io/twitter/follow/artemis__xyz?link=https%3A%2F%2Ftwitter.com%2Fartemis__xyz

