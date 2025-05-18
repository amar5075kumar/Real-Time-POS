## Overview
This  Real-Time Point-of-Sale Solution utilizing Delta Live Tables (DLT) with Amazon Managed Workflows for Apache Managed Streaming Kafka(MSK). This solution showcases how Delta Live Tables can be utilized to construct a near real-time lakehouse architecture for calculating current inventories of various products across multiple store locations. Instead of directly transitioning from raw data ingestion to inventory calculations, I've structured this solution into two distinct phases.


<img src='https://brysmiwasb.blob.core.windows.net/demos/images/pos_dlt_pipeline_UPDATED.png' width=800>

## Introduction
The initial phase, known as Bronze-to-Silver ETL, involves transforming ingested data to enhance accessibility. The actions performed on the data at this stage, such as breaking down nested arrays and removing duplicate records, do not involve applying any business-driven interpretations. The tables generated in this phase represent the Silver layer of our lakehouse architecture.

In the subsequent phase, referred to as Silver-to-Gold ETL, the Silver tables are leveraged to derive the business-aligned output, which is the calculated current-state inventory. The resulting data is stored in a table representing the Gold layer of our architecture.

Throughout this two-phase workflow, I employ Delta Live Tables (DLT) for orchestration and monitoring.

## *Spark Structured Streaming vs. DLT(Delta Live Table)*

### *Technology Stack*:

Spark Structured Streaming is a component of Apache Spark that enables scalable, fault-tolerant stream processing with a familiar SQL-like interface.
Delta Live Tables (DLT) is a higher-level abstraction built on top of Spark that provides orchestration, monitoring, and management capabilities for streaming workflows.

### *Functionality*:
Spark Structured Streaming focuses on stream processing and provides APIs for defining and executing streaming computations on data streams.
DLT extends the capabilities of Spark Structured Streaming by offering features such as job scheduling, orchestration of workflows, and monitoring of streaming data pipelines.

### *Ease of Use*:
Spark Structured Streaming requires developers to write code to define streaming queries and manage the execution of those queries.
DLT simplifies the development and management of streaming workflows by providing a higher-level interface for defining and orchestrating streaming jobs.

### *Integration*:
Spark Structured Streaming integrates seamlessly with the Apache Spark ecosystem and can leverage Spark's extensive library of connectors and processing functions.
DLT integrates with Spark but adds additional functionality specific to managing streaming workflows, such as job scheduling and monitoring.
Scalability:

Both Spark Structured Streaming and DLT are designed for scalability and can handle large-scale streaming data processing tasks.
DLT's additional management features can help optimize the scalability and performance of streaming workflows.

By incorporating Delta Live Tables (DLT), the implementation of the streaming workflows remains consistent. DLT acts as a wrapper around our workflows, enabling orchestration, monitoring, and other enhancements that would otherwise require additional implementation efforts. In this context, DLT complements Spark Structured Streaming rather than replacing it. 

## Definition

* *Notebook_01: Env Setup*
* *Notebook_02: Generating_Raw_Data*
* *Notebook_03: ETL for Broze Layer to Silver Layer*
* *Notebook_04: ETL for Silver to Gold Layer *
* *Notebook_05: Orchestration- JOB Scheduling_DLT*






