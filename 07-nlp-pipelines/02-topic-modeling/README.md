# Assignment 02 — Topic Modeling with Apache Spark and Spark NLP

![Spark](https://img.shields.io/badge/Apache-Spark-orange) ![PySpark](https://img.shields.io/badge/Language-PySpark-blue) ![ML](https://img.shields.io/badge/PySpark-ML-purple) ![NLP](https://img.shields.io/badge/Spark-NLP-red) ![LDA](https://img.shields.io/badge/MLlib-LDA-blueviolet)

**Notebook:** `BA_Ex5-2_rrata.ipynb`

---

## Overview

This assignment builds a distributed Topic Modeling pipeline using Spark NLP for text preprocessing and Spark MLlib's LDA (Latent Dirichlet Allocation) to discover latent topics in two corpora: an Australian news headlines dataset and a COVID-19 tweets dataset. The assignment includes systematic hyperparameter tuning across both datasets to find the best `k` (number of topics) and `maxIter` (training iterations).

---

## Background

### Topic Modeling

Topic modeling is an unsupervised technique that discovers hidden thematic structure in large text collections by identifying groups of words that frequently co-occur. It is used to organize and summarize large unstructured corpora into a smaller, interpretable set of topics.

### LDA vs. LSA

**Latent Semantic Analysis (LSA)** reduces matrix dimensionality to find low-dimensional representations of documents and words — useful for classification but limited in interpretability.

**Latent Dirichlet Allocation (LDA)** is a fully probabilistic unsupervised model designed specifically for topic discovery. It assumes each document is a mixture of topics and each topic is a distribution over words.

### Spark MLlib LDA

Spark MLlib's distributed LDA implementation is optimized for large-scale processing, enabling topic modeling over billions of documents by distributing computation across a cluster.

### Evaluation Metrics

**Log Likelihood** — measures how well the model fits the training data. Higher is better.

**Log Perplexity** — measures how well the model predicts unseen data. Lower is better.

---

## Part 1: News Headlines Dataset

### Environment Setup

```python
import os
! apt-get update -qq
! apt-get install -y openjdk-8-jdk-headless -qq > /dev/null

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

! pip install --ignore-installed pyspark==3.5.3
! pip install --ignore-installed spark-nlp==5.3.2
```

### Import Packages and Initialize Spark

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp

spark = sparknlp.start()
# Python 3.12.12 | Spark NLP: 5.3.2 | Apache Spark: 3.5.1
```

### Download the News Dataset

```python
from pathlib import Path
import urllib.request

download_path = "./abcnews-date-text.csv"
if not Path(download_path).is_file():
    url = "..."
    urllib.request.urlretrieve(url, download_path)
```

### Read the Data

The CSV is loaded with schema inference, a header row, and comma delimiter. The resulting DataFrame has two columns: `publish_date` and `headline_text`.

```python
df = spark.read \
    .option("inferSchema", True) \
    .option("header", True) \
    .option("delimiter", ",") \
    .csv("abcnews-date-text.csv")

df.show(10)
```

### Spark NLP Preprocessing Pipeline

The pipeline chains six annotators in sequence:

#### 1. Document Assembler
Converts raw text strings into the Spark NLP `document` format. The `shrink` cleanup mode removes extra whitespace, tabs, and newlines.

```python
document_assembler = DocumentAssembler() \
    .setInputCol("headline_text") \
    .setOutputCol("document") \
    .setCleanupMode("shrink")
```

#### 2. Tokenizer
Splits document text into individual word tokens.

```python
tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")
```

#### 3. Normalizer
Removes punctuation, numbers, special characters, and converts to lowercase.

```python
normalizer = Normalizer() \
    .setInputCols(["token"]) \
    .setOutputCol("normalized") \
    .setLowercase(True)
```

#### 4. Stop Words Cleaner
Removes common stopwords (e.g., "the", "a", "in") that carry no semantic meaning.

```python
stopwords_cleaner = StopWordsCleaner() \
    .setInputCols(["normalized"]) \
    .setOutputCol("cleanTokens") \
    .setCaseSensitive(False)
```

#### 5. Stemmer
Reduces words to their root form (e.g., "running" → "run") to consolidate related word forms.

```python
stemmer = Stemmer() \
    .setInputCols(["cleanTokens"]) \
    .setOutputCol("stem")
```

#### 6. Finisher
Converts Spark NLP annotation objects back to a plain array column for downstream MLlib processing.

```python
finisher = Finisher() \
    .setInputCols(["stem"]) \
    .setOutputCols(["tokens"]) \
    .setOutputAsArray(True) \
    .setCleanAnnotations(False)
```

### Build and Run the ML Pipeline

```python
nlp_pipeline = Pipeline(stages=[
    document_assembler,
    tokenizer,
    normalizer,
    stopwords_cleaner,
    stemmer,
    finisher
])

nlp_model = nlp_pipeline.fit(df)
processed_df = nlp_model.transform(df)
tokens_df = processed_df.select('publish_date', 'tokens')
tokens_df.show(10000)
```

### Feature Engineering

`CountVectorizer` converts the token arrays into sparse numerical feature vectors required by LDA.

```python
from pyspark.ml.feature import CountVectorizer

cv = CountVectorizer(inputCol="tokens", outputCol="features", vocabSize=500, minDF=3.0)
cv_model = cv.fit(tokens_df)
vectorized_tokens = cv_model.transform(tokens_df)
```

- `vocabSize=500` — limits the vocabulary to the top 500 most frequent words
- `minDF=3.0` — a word must appear in at least 3 documents to be included

### LDA Model and Hyperparameter Tuning

The baseline LDA model uses `k=3` topics and `maxIter=10`. Assignment 5 required testing at least 5 combinations to find the best hyperparameters.

```python
from pyspark.ml.clustering import LDA

lda = LDA(k=num_topics, maxIter=10)
model = lda.fit(vectorized_tokens)
ll = model.logLikelihood(vectorized_tokens)
lp = model.logPerplexity(vectorized_tokens)
```

#### Hyperparameter Search Results

**Varying k (with increasing maxIter):**

| Test | k | maxIter | Log Likelihood | Log Perplexity | Notes |
|------|---|---------|---------------|----------------|-------|
| 1 | 5 | 20 | — | — | Perplexity increases with k |
| 2 | 10 | 40 | — | — | Perplexity continues to rise |
| 3 | 20 | 60 | — | — | Diminishing returns |
| 4 | 30 | 80 | — | — | Worst perplexity |
| 5 | 15 | 20 | — | — | Better than k=30, worse than k=3 |

**Conclusion from k sweep:** Higher k consistently increased perplexity. The data is best represented with `k=3` topics.

**Fixing k=3, varying maxIter:**

| Test | k | maxIter | Log Likelihood | Log Perplexity |
|------|---|---------|---------------|----------------|
| 6 | 3 | 20 | — | — |
| 7 | 3 | 30 | — | — |
| 8 | 3 | 40 | — | — |
| 9 | 3 | 50 | — | — |
| 10 | 3 | 60 | — | — |
| 11 | 3 | 70 | — | — |

**Conclusion:** Increasing `maxIter` incrementally improved both metrics each time (likelihood increased, perplexity decreased). Improvements became marginal by `maxIter=70`.

**Best hyperparameters for news headlines: `k=3`, `maxIter=70`**

### Visualize Topics

```python
vocab = cv_model.vocabulary
topics = model.describeTopics()
topics_rdd = topics.rdd \
    .map(lambda row: row['termIndices']) \
    .map(lambda idx_list: [vocab[idx] for idx in idx_list]) \
    .collect()

for idx, topic in enumerate(topics_words):
    print("topic: {}".format(idx))
    print("*" * 25)
    for word in topic:
        print(word)
```

**Discovered topics (news headlines, k=3):**

| Topic 0 | Topic 1 | Topic 2 |
|---------|---------|---------|
| council | u | polic |
| iraqi | war | man |
| win | iraq | govt |
| fire | sai | plan |
| iraq | new | baghdad |
| kill | mai | death |
| fund | world | face |
| nsw | warn | get |
| claim | protest | charg |
| report | anti | court |

Topics reflect the news era of the dataset — Iraq War coverage (Topics 0 & 1) and domestic government/crime news (Topic 2).

---

## Part 2 (Assignment 6): Coronavirus Tweets Dataset

The same pipeline is applied to 1,000 coronavirus tweets (`coronavirus-text-only-1000.txt`) obtained from the course cluster via `scp`.

### Read the Data

```python
from pyspark.sql.functions import col

df = spark.read.text('coronavirus-text-only-1000.txt')
df = df.withColumnRenamed("value", "document")
df = df.filter(col("document") != "text")
df = df.filter(col("document").isNotNull() & (col("document") != ""))

df.show(5, truncate=False)
df.count()  # 999
```

### Preprocessing Differences

The pipeline is identical to Part 1 with two changes:

1. The `DocumentAssembler` input column is `"document"` (renamed from `"value"`)
2. `"coronavirus"` is added as a custom stopword since it appears in every tweet and carries no discriminating information:

```python
stopwords_cleaner = StopWordsCleaner() \
    .setInputCols(["normalized"]) \
    .setOutputCol("cleanTokens") \
    .setCaseSensitive(False) \
    .setStopWords(StopWordsCleaner.loadDefaultStopWords("english") + ["coronavirus"])
```

### Pipeline and Feature Engineering

The same `Pipeline`, `CountVectorizer` (`vocabSize=500`, `minDF=3.0`), and `LDA` setup is used.

Baseline: `k=3`, `maxIter=10`
- Log Likelihood: `-73196.00747946383`
- Log Perplexity: `5.4591294361175295`

### Hyperparameter Search Results

| Test | k | maxIter | Log Likelihood | Log Perplexity |
|------|---|---------|---------------|----------------|
| 1 | 5 | 20 | -70366.94 | 5.2481 |
| 2 | 10 | 30 | — | — |
| 3 | 7 | 25 | — | — |
| 4 | 5 | 25 | — | — |
| 5 | 5 | 30 | — | — |
| 6 | 5 | 40 | -70366.94 | 5.2481 |

**Additional maxIter tests at k=3:**

| maxIter | Log Likelihood | Log Perplexity |
|---------|---------------|----------------|
| 30 | -176992.53 | 6.2383 |
| 40 | -176414.45 | 6.2179 |
| 50 | -175978.59 | 6.2025 |
| 60 | -175617.42 | 6.1899 |

**Conclusion:** When k was increased, log likelihood worsened. The ideal k for this corpus was `k=5`. With `k=5` fixed, increasing `maxIter` improved both metrics. Improvements leveled off around `maxIter=40`.

**Best hyperparameters for tweets: `k=5`, `maxIter=40`**

### Visualize Topics

The same vocabulary extraction and topic visualization code is used. The top words for each of the 5 discovered topics reflect distinct coronavirus discussion themes such as health measures, government response, social media discourse, economic impact, and misinformation.

---

## Key Concepts

**DocumentAssembler** — Entry point for every Spark NLP pipeline. Converts a raw string column into the internal `document` annotation format.

**Normalizer** — Strips punctuation, numbers, and special characters; lowercases text. Reduces noise before downstream modeling.

**StopWordsCleaner** — Removes high-frequency, semantically empty words. Custom stopwords can be appended (e.g., `"coronavirus"` for domain-specific noise).

**Stemmer** — Reduces inflected words to their root form. Consolidates token variants so the model treats "running", "runs", and "run" as the same feature.

**Finisher** — Converts Spark NLP annotation structs back to plain string arrays, bridging the gap between Spark NLP annotators and Spark MLlib feature transformers.

**CountVectorizer** — Converts token arrays to sparse TF (term frequency) feature vectors. `vocabSize` caps the vocabulary; `minDF` filters rare terms.

**LDA** — Probabilistic unsupervised model that represents each document as a mixture of `k` topics and each topic as a distribution over vocabulary words.
