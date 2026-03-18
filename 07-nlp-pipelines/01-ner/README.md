# Assignment 01 — Named Entity Recognition with BERT and Spark NLP

**Notebooks:** `EX5-1_rrata.ipynb` · `EX5-1_Q10_rrata.ipynb`

---

## Overview

This assignment builds a Named Entity Recognition (NER) pipeline using BERT contextual embeddings and Spark NLP's `NerDLApproach` deep learning annotator, trained on the CoNLL 2003 benchmark dataset. A second notebook applies a fully pretrained `recognize_entities_dl` pipeline to annotate a Wikipedia excerpt without any custom training.

---

## Background

### Named Entity Recognition

NER is a sequence labeling task that locates and classifies named entities in unstructured text into predefined categories — persons, organizations, locations, dates, monetary values, and more.

### BERT for NER

The architecture is inspired by Chiu & Nichols (*Named Entity Recognition with Bidirectional LSTM-CNN*), which automatically detects word- and character-level features using a hybrid BiLSTM-CNN model. Here, pretrained BERT contextual embeddings replace static embeddings, providing richer token representations that capture surrounding context.

---

## Part 1: Custom NER with NerDLApproach

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

> Earlier versions (`pyspark==3.1.2`, `spark-nlp==3.4.3`) caused dependency conflicts. Versions 3.5.1 and 5.3.3 were used instead. Packages must be reinstalled after each Colab session disconnect.

### Import Packages and Initialize Spark

```python
from pyspark.sql import SparkSession
from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *
import sparknlp

spark = sparknlp.start(memory='12g')
print("Spark NLP version: ", sparknlp.version())   # 5.3.3
print("Apache Spark version: ", spark.version)     # 3.5.1
```

### Download the CoNLL 2003 Training Dataset

```python
from pathlib import Path
import urllib.request

download_path = "./eng.train"
if not Path(download_path).is_file():
    print("File Not found will downloading it!")
    url = "https://github.com/patverga/torch-ner-nlp-from-scratch/raw/master/data/conll2003/eng.train"
    urllib.request.urlretrieve(url, download_path)
else:
    print("File already present.")
```

### Load into Spark DataFrame

The `CoNLL` class reads `eng.train` and produces a structured DataFrame with columns for `text`, `document`, `sentence`, `token`, `pos`, and `label`.

```python
from sparknlp.training import CoNLL

training_data = CoNLL().readDataset(spark, './eng.train')
training_data.show()
# only showing top 20 rows
```

### BERT Word Embeddings

A pretrained `bert_base_cased` model (384.9 MB) generates contextual embeddings for each token. The embeddings are stored in a new `bert` column.

```python
bert = BertEmbeddings.pretrained('bert_base_cased', 'en') \
    .setInputCols(["sentence", "token"]) \
    .setOutputCol("bert") \
    .setCaseSensitive(False)
```

### Configure NerDLApproach

`NerDLApproach` is Spark NLP's deep learning NER annotator. It takes BERT embeddings as input features and trains a model to predict IOB2 entity tags.

```python
nerTagger = NerDLApproach() \
    .setInputCols(["sentence", "token", "bert"]) \
    .setLabelColumn("label") \
    .setOutputCol("ner") \
    .setMaxEpochs(1) \
    .setRandomSeed(0) \
    .setVerbose(1) \
    .setValidationSplit(0.2) \
    .setEvaluationLogExtended(True) \
    .setEnableOutputLogs(True) \
    .setIncludeConfidence(True) \
    .setTestDataset("test_withEmbeds.parquet")
```

**Key parameters:**

| Parameter | Value | Description |
|-----------|-------|-------------|
| `.setInputCols` | `["sentence", "token", "bert"]` | Feature columns consumed by the model |
| `.setLabelColumn` | `"label"` | Ground-truth IOB2 entity labels |
| `.setOutputCol` | `"ner"` | Column where predictions are written |
| `.setMaxEpochs` | `1` | Training epochs (limited for speed) |
| `.setValidationSplit` | `0.2` | 20% of training data held out for validation |
| `.setEvaluationLogExtended` | `True` | Per-label evaluation metrics in logs |
| `.setEnableOutputLogs` | `True` | Writes training logs to home folder |
| `.setIncludeConfidence` | `True` | Confidence scores included in output |
| `.setTestDataset` | `"test_withEmbeds.parquet"` | Pre-embedded test set for mid-training evaluation |

### Prepare the Test Dataset

Download `eng.testa`, apply BERT embeddings, and save as Parquet for efficient reuse:

```python
download_path = "./eng.testa"
if not Path(download_path).is_file():
    url = "https://github.com/patverga/torch-ner-nlp-from-scratch/raw/master/data/conll2003/eng.testa"
    urllib.request.urlretrieve(url, download_path)

test_data = CoNLL().readDataset(spark, './eng.testa')
test_data = bert.transform(test_data)
test_data.write.mode("overwrite").parquet("test_withEmbeds.parquet")
```

### Train the NER Model

A Spark NLP `Pipeline` chains the BERT embeddings and NER tagger. Training is limited to 500 rows for speed (wall time: ~4 min 34 sec).

```python
%%time
ner_pipeline = Pipeline(stages=[bert, nerTagger])
ner_model = ner_pipeline.fit(training_data.limit(500))
```

### Predictions

```python
predictions = ner_model.transform(test_data.select("sentence", "token", "label"))
predictions.show()
```

To view token-level ground truth vs. predictions side by side:

```python
import pyspark.sql.functions as F

predictions.select(
    F.explode(
        F.arrays_zip(
            F.col('token.result').alias('token_result_col'),
            F.col('label.result').alias('label_result_col'),
            F.col('ner.result').alias('ner_result_col')
        )
    ).alias("cols")
).select(
    F.col("cols.token_result_col").alias("token"),
    F.col("cols.label_result_col").alias("ground_truth"),
    F.col("cols.ner_result_col").alias("prediction")
).show(truncate=False)
```

**Sample output:**

| token | ground_truth | prediction |
|-------|-------------|------------|
| CRICKET | O | O |
| LEICESTERSHIRE | I-ORG | I-ORG |
| LONDON | I-LOC | I-LOC |
| Phil | I-PER | I-PER |
| Simmons | I-PER | I-PER |
| West | I-MISC | I-PER |
| Indian | I-MISC | I-PER |

Most entities are correctly identified. Mismatches (e.g. `I-MISC` predicted as `I-PER`) reflect the limited 1-epoch training run on 500 rows.

---

## Part 2 (Assignment 10): PretrainedPipeline on Wikipedia Text

Rather than training a custom model, this notebook uses Spark NLP's `recognize_entities_dl` pretrained pipeline — a ready-made pipeline including tokenization, embeddings, and entity recognition.

### Setup

```python
from sparknlp.pretrained import PretrainedPipeline

spark = sparknlp.start(memory='12g')
pipeline = PretrainedPipeline("recognize_entities_dl", lang="en")
# Approx size to download: 159 MB
```

### Input Text (Wikipedia — University of Illinois Springfield)

```
The University of Illinois Springfield (UIS) is a public university in Springfield, Illinois,
United States. The university was established in 1969 as Sangamon State University by the
Illinois General Assembly and became a part of the University of Illinois system on July 1, 1995.
As a public liberal arts college and the newest campus in the University of Illinois system,
UIS is a member of the Council of Public Liberal Arts Colleges. President: Timothy L. Killeen.
Chancellor: Janet L. Gooch. Location: Springfield, Illinois, United States.
```

### Annotate and Display

```python
result = pipeline.annotate(text)

for token, ner in zip(result['token'], result['ner']):
    print(f"{token:20} --> {ner}")
```

### Sample Output

| Token | NER Tag | Entity |
|-------|---------|--------|
| University | B-ORG | University of Illinois Springfield |
| of | I-ORG | |
| Illinois | I-ORG | |
| Springfield | I-ORG | |
| UIS | B-ORG | UIS (abbreviation) |
| Springfield | B-LOC | Springfield, Illinois, United States |
| Illinois | B-LOC | |
| United | B-LOC | |
| States | I-LOC | |
| Sangamon | B-ORG | Sangamon State University |
| Timothy | B-PER | Timothy L. Killeen |
| L. | I-PER | |
| Killeen | I-PER | |
| Janet | B-PER | Janet L. Gooch |
| Gooch | I-PER | |

The pretrained pipeline correctly extracts organizations, locations, and persons from the Wikipedia text with no custom training required.

---

## Key Concepts

**CoNLL 2003** — Benchmark NER dataset. Uses IOB2 tagging: `B-` begins an entity span, `I-` continues it, `O` marks non-entities. Types: `PER`, `ORG`, `LOC`, `MISC`.

**BERT** — Bidirectional transformer that generates context-aware token embeddings. The same token receives different vectors depending on surrounding words.

**NerDLApproach** — Spark NLP deep learning NER annotator. Trains a BiLSTM-CNN-CRF-style model using BERT embeddings as input features.

**PretrainedPipeline** — A fully packaged Spark NLP pipeline loaded in one line. No training required — useful for rapid annotation and prototyping.

**Parquet** — Columnar storage format used to persist the BERT-embedded test dataset, avoiding redundant computation during training evaluation.
