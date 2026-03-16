# Big Data Analytics
End-to-end Hadoop ecosystem coursework — distributed storage, ETL pipelines, NoSQL databases, and large-scale text processing

![Hadoop](https://img.shields.io/badge/Hadoop-3.x-blue)
![Spark](https://img.shields.io/badge/Apache-Spark-orange)
![Hive](https://img.shields.io/badge/Hive-HBase-green)

## Overview
This repository documents hands-on coursework from a graduate-level Data Analytics course covering the full Hadoop ecosystem. Projects span cluster setup, distributed file operations, ETL pipelines, NoSQL data storage, SQL-on-Hadoop analytics, Spark batch processing, and NLP-style document processing. Each module is self-contained with its own code and documentation.

## Modules

| # | Module | Tools |
|---|--------|-------|
| 01 | **Hadoop cluster setup** — Multi-node cluster config: NameNode, DataNodes, YARN, SSH setup | Hadoop, YARN, HDFS |
| 02 | **HDFS word count** — MapReduce word count over distributed files | MapReduce, Python, HDFS |
| 03 | **ETL pipeline** — Multi-step load/filter/transform/store pipeline in Pig Latin | Apache Pig, HDFS |
| 04 | **NoSQL — HBase** — Column-family schema design, row key strategy, CRUD operations | HBase, Hadoop |
| 05 | **SQL on Hadoop — Hive** — HiveQL queries on partitioned HDFS tables | Hive, HiveQL |
| 06 | **Batch processing — Apache Spark** — RDD and DataFrame transformations in PySpark | PySpark, Spark |
| 07 | **Text & document processing** — TF-IDF and document similarity over a corpus | Spark MLlib, NLP |

## Tech stack
Hadoop 3.x · HDFS · YARN · MapReduce · Apache Spark / PySpark · Hive · HBase · Apache Pig · Python

## Relevance to healthcare data engineering
Healthcare analytics increasingly relies on distributed systems for large EHR datasets, claims data, and genomic records that exceed single-machine capacity. The patterns here — ETL pipelines, columnar NoSQL storage, SQL-on-Hadoop querying, and batch processing — map directly to tools used in clinical data warehousing and population health platforms (e.g. Cloudera, AWS EMR, Azure HDInsight).

## Structure
Each subdirectory contains code, sample data where applicable, and a module README with setup instructions and key concepts.
