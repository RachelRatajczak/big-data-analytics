# 02 — HDFS word count
MapReduce word frequency analysis over HDFS — nursery rhyme dataset and real-world COVID-19 corpus

![HDFS](https://img.shields.io/badge/Hadoop-HDFS-blue) ![MapReduce](https://img.shields.io/badge/MapReduce-Java-purple)

## Overview
This module demonstrates the classic MapReduce word count program on a multi-node Hadoop cluster. Word count is the "Hello World" of distributed computing — it shows how Hadoop splits input data across nodes, runs parallel map tasks, shuffles and sorts intermediate results, then reduces them to a final word frequency output. Run on two datasets: a small nursery rhyme for validation, and a 1,000-article COVID-19 text corpus for real-world scale.

## MapReduce pipeline
```
Input file → Mapper (emit word,1) → Shuffle & sort → Reducer (sum counts) → Output (part-r-00000)
```

## Datasets
| Dataset | Description |
|---------|-------------|
| `MaryHadALittleLamb.txt` | Small nursery rhyme — validates pipeline before scaling up |
| `coronavirus-text-only-1000.txt` | 1,000 COVID-19 news articles — real-world word frequency at scale |

---

## Part 1 — nursery rhyme dataset

### Set up HDFS and load data
```bash
mkdir CSC534BDA && cd CSC534BDA
hadoop fs -mkdir /user/csc/WordCount
hadoop fs -ls /user/csc/
hadoop fs -put MaryHadALittleLamb.txt /user/csc/WordCount/
hadoop fs -cat /user/csc/WordCount/MaryHadALittleLamb.txt
hadoop fs -get /user/csc/WordCount/MaryHadALittleLamb.txt ./
```

### Set environment variables
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
```
`HADOOP_CLASSPATH` ensures Hadoop uses the correct JDK version.

### Compile and create JAR
```bash
cp WordCount.java ~/CSC534BDA/
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
```

### Run
```bash
hadoop jar wc.jar WordCount \
  /user/csc/WordCount/MaryHadALittleLamb.txt \
  /user/csc/WordCount/output
```

### View output
```bash
hadoop fs -cat /user/csc/WordCount/output/part-r-00000
hadoop fs -cat /user/csc/WordCount/output/*
```
Output is split across `part-r-*` files due to parallel processing.

---

## Part 2 — COVID-19 corpus
```bash
hadoop fs -mkdir /user/csc/COVID19
hadoop fs -put coronavirus-text-only-1000.txt /user/csc/COVID19/
hadoop jar wc.jar WordCount \
  /user/csc/COVID19/coronavirus-text-only-1000.txt \
  /user/csc/COVID19/output
hadoop fs -cat /user/csc/COVID19/output/part-r-00000 | head -10
```

---

## Key HDFS commands
| Command | Purpose |
|---------|---------|
| `-mkdir` | Create directory in HDFS |
| `-put` | Upload local file → HDFS |
| `-get` | Download HDFS file → local |
| `-cat` | Print file contents to stdout |
| `-ls` | List files in HDFS path |
| `-rm -r` | Delete file or directory from HDFS |

---

## Key concepts demonstrated
- HDFS stores files as blocks distributed and replicated across DataNodes
- MapReduce map tasks run in parallel — each processes a subset of input independently
- Shuffle and sort groups identical keys before passing to reducers
- Multiple `part-r-*` output files reflect the number of reducers used
- Medical/news text corpora are a natural fit for distributed word frequency analysis
