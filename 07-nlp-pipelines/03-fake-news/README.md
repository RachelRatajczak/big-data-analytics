# Assignment 03 — Detecting Fake News with Apache Spark and Spark NLP

**Notebook:** `BA_Ex5-3_rrata.ipynb`

---

## Overview

This assignment applies pretrained Spark NLP models to classify text as fake or real news across three datasets: a synthetic 10-row text list, Trump and Biden tweet datasets, and a labeled fake/real news CSV. Rather than training a model from scratch, all three tasks use pretrained Universal Sentence Encoder (USE) embeddings paired with a John Snow Labs pretrained deep learning classifier.

---

## Background

### Pretrained Pipeline Approach

Unlike the NER assignment (which trained a custom model) or the topic modeling assignment (which used unsupervised LDA), fake news detection here is a supervised classification task using fully pretrained components. The pipeline requires no labeled training data from the user — the classifier was trained externally on 2016 election-related datasets by John Snow Labs.

### Universal Sentence Encoder (USE)

USE encodes full sentences into fixed-length 512-dimensional vectors that capture semantic meaning. These sentence-level embeddings are richer than token-level embeddings for classification tasks because they represent the entire sentence's meaning in a single vector.

---

## Part 1: Synthetic Text List (Core Exercise)

### Environment Setup

```python
import os
! apt-get update -qq
! apt-get install -y openjdk-8-jdk-headless -qq > /dev/null
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
! pip install --ignore-installed pyspark==3.5.1
! pip install --ignore-installed spark-nlp==5.3.3
```

### Import Packages

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp

spark = sparknlp.start()
```

### Create Dataset

A 10-row text list from the John Snow Labs Colab tutorial is converted to a Spark DataFrame:

```python
text_list = [...]  # 10 sample news texts
df = spark.createDataFrame(text_list, ["text"])
df.printSchema()
df.count()  # 10
```

### Preprocessing Pipeline

```python
# Document Assembler
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

# Universal Sentence Encoder (pretrained)
use = UniversalSentenceEncoder.pretrained() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence_embeddings")

# Pretrained fake news classifier
classifier = ClassifierDLModel.pretrained('classifierdl_use_fakenews', 'en') \
    .setInputCols(["sentence_embeddings"]) \
    .setOutputCol("class")

# Pipeline
nlp_pipeline = Pipeline(stages=[document_assembler, use, classifier])
```

### Apply the Pipeline

Since all models are pretrained, no actual training occurs. A dummy DataFrame is needed to satisfy Spark ML's `.fit()` requirement:

```python
# Dummy fit
dummy_df = spark.createDataFrame([[""]],  ["text"])
pipeline_model = nlp_pipeline.fit(dummy_df)

# Transform real data
result_df = pipeline_model.transform(df)
```

### Visualize Results

```python
import pyspark.sql.functions as F

result_df.select(
    F.explode(
        F.arrays_zip(
            F.col('class.result').alias('prediction'),
            F.col('document.result').alias('document')
        )
    ).alias("cols")
).select(
    F.col("cols.prediction").alias("Prediction"),
    F.col("cols.document").alias("Document")
).show(truncate=False)
```

---

## Part 2 (Assignment 5): Trump and Biden Tweet Datasets

### Step 1: Environment Setup

Same Java/PySpark/Spark NLP installation as above.

### Step 2: Import Packages

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from sparknlp.base import *
from sparknlp.annotator import *
import sparknlp
import requests

spark = sparknlp.start()
```

### Step 3: Download and Load Tweet Data

Two separate tweet datasets are downloaded via HTTP requests. The data is kept separate — datasets are **not** combined.

```python
candidates = ["trump", "biden"]
data = {}

for i, candidate in enumerate(candidates):
    url = f"https://raw.githubusercontent.com/.../{candidate}_tweets.csv"
    response = requests.get(url)
    data[candidate] = response.text

trump_list = data["trump"].split("\n")
biden_list = data["biden"].split("\n")

trump_df = spark.createDataFrame([[t] for t in trump_list], ["text"])
biden_df = spark.createDataFrame([[b] for b in biden_list], ["text"])

trump_df.printSchema()
trump_df.count()

biden_df.printSchema()
biden_df.count()
```

### Step 4: Preprocessing Pipeline

Same pipeline as Part 1 — `DocumentAssembler` → `UniversalSentenceEncoder` → `ClassifierDLModel`.

```python
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

use = UniversalSentenceEncoder.pretrained() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence_embeddings")

classifier = ClassifierDLModel.pretrained('classifierdl_use_fakenews', 'en') \
    .setInputCols(["sentence_embeddings"]) \
    .setOutputCol("class")

nlp_pipeline = Pipeline(stages=[document_assembler, use, classifier])
```

### Step 5: Apply the Pipeline

```python
dummy_df = spark.createDataFrame([[""]],  ["text"])
pipeline_model = nlp_pipeline.fit(dummy_df)

trump_result = pipeline_model.transform(trump_df)
biden_result = pipeline_model.transform(biden_df)
```

Schemas of both result DataFrames are printed to verify the transformation.

### Step 6: Visualize Results

The same SQL functions approach is used for each candidate separately:

```python
for result, label in [(biden_result, "Biden"), (trump_result, "Trump")]:
    print(f"\n--- {label} Results ---")
    result.select(
        F.explode(
            F.arrays_zip(
                F.col('class.result').alias('prediction'),
                F.col('document.result').alias('document')
            )
        ).alias("cols")
    ).select(
        F.col("cols.prediction").alias("Prediction"),
        F.col("cols.document").alias("Document")
    ).show(5, truncate=False)
```

> **Note:** Classification performance may not be high because the pretrained model was trained on 2016 election-related datasets. Tweet content from Trump/Biden datasets may differ stylistically from the training distribution.

---

## Part 3 (Assignment 6): Fake/Real News CSV Dataset

### Step 1: Version Adjustment

Dependency issues with the pretrained `.fit()` step required updated versions:

```python
! pip install --ignore-installed spark-nlp==5.4.0
! pip install --ignore-installed pyspark==3.5.0
```

### Step 2: Import Packages

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
```

### Step 3: Start Spark Session

```python
spark = sparknlp.start()
```

### Step 4: Select Pretrained Model

The `classifierdl_use_fakenews` model from John Snow Labs is the classifier used for prediction. It uses USE embeddings as input features.

### Step 5: Upload the CSV

The fake/real news CSV file is uploaded from the local filesystem using Google Colab's file upload utility:

```python
from google.colab import files
uploaded = files.upload()
```

### Step 6: Read and Clean the DataFrame

The CSV has multiple lines per record, double quotes, and escape characters that require specific read options:

```python
df = spark.read \
    .option("inferSchema", True) \
    .option("header", True) \
    .option("delimiter", ",") \
    .option("multiLine", True) \
    .option("escape", '"') \
    .csv("fake_or_real_news.csv")

df = df.filter(df["text"].isNotNull())
df = df.drop("_c0")

df.printSchema()
df.show(5, truncate=True)
df.count()
```

> **Key challenge:** The CSV contains multi-line news article text with embedded quotes. `multiLine=True` and `escape='"'` are required to parse it correctly.

### Step 7: Preprocessing Pipeline

```python
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

use = UniversalSentenceEncoder.pretrained() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence_embeddings")

classifier = ClassifierDLModel.pretrained('classifierdl_use_fakenews', 'en') \
    .setInputCols(["sentence_embeddings"]) \
    .setOutputCol("class")

nlp_pipeline = Pipeline(stages=[document_assembler, use, classifier])
```

### Step 8: Run the Pipeline

```python
dummy_df = spark.createDataFrame([[""]],  ["text"])
pipeline_model = nlp_pipeline.fit(dummy_df)
result_df = pipeline_model.transform(df)
```

A verification step displays `result_df` contents to confirm the pretrained models are producing output before visualization.

### Step 9: Visualize Results

```python
result_df.select(
    F.explode(
        F.arrays_zip(
            F.col('class.result').alias('class'),
            F.col('document.result').alias('document')
        )
    ).alias("cols")
).select(
    F.col("cols.class").alias("class"),
    F.col("cols.document").alias("document")
).show(5, truncate=True)
```

---

## Pipeline Architecture Summary

All three parts share the same core pipeline structure:

```
Raw Text
    ↓
DocumentAssembler      — converts string column to Spark NLP document format
    ↓
UniversalSentenceEncoder (pretrained, ~150MB)
                       — encodes full sentences into 512-dim semantic vectors
    ↓
ClassifierDL (pretrained, classifierdl_use_fakenews)
                       — predicts "FAKE" or "REAL" from sentence embeddings
    ↓
Prediction Output
```

---

## Key Concepts

**Universal Sentence Encoder (USE)** — Pretrained transformer that encodes entire sentences into fixed-length 512-dimensional vectors capturing semantic meaning. Outperforms token-level embeddings for sentence classification tasks.

**ClassifierDL** — Spark NLP's deep learning text classifier. When loaded with `classifierdl_use_fakenews`, it predicts fake vs. real news from USE embeddings. Pretrained on 2016 election-related data by John Snow Labs.

**Dummy fit pattern** — Since all models are pretrained, `nlp_pipeline.fit()` does not train anything. Spark ML requires a `.fit()` call to return a fitted model object, so a single-row dummy DataFrame is passed to satisfy this requirement without affecting the pretrained weights.

**`multiLine` CSV option** — Required when reading CSV files where a single field spans multiple lines (e.g., long news articles with embedded newlines). Combined with `escape='"'` to handle quoted strings containing commas.

**`arrays_zip` + `explode`** — A Spark SQL pattern for pairing and flattening parallel arrays (e.g., predictions and documents) into a clean per-row output table for display.

## Notes on Model Performance

The `classifierdl_use_fakenews` model was trained on 2016 U.S. election-related text. When applied to:
- **Tweet data** (Parts 1 and 2) — performance may be lower due to domain mismatch between short informal tweets and the longer formal news articles the model was trained on
- **Fake/Real news CSV** (Part 3) — performance should be stronger as the data format is more similar to the training distribution
