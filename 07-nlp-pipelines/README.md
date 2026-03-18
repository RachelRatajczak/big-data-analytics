# Module 07 — NLP Pipelines: NER, Topic Modeling & Classification

**Large-scale text processing with Spark NLP, BERT, and Spark MLlib**

> Graduate coursework — University of Illinois Springfield, Data Analytics (CSC 534)

![Spark](https://img.shields.io/badge/Apache-Spark-orange) ![PySpark](https://img.shields.io/badge/Language-PySpark-blue) ![ML](https://img.shields.io/badge/PySpark-ML-purple) ![NLP](https://img.shields.io/badge/Spark-NLP-red) ![Colab](https://img.shields.io/badge/Platform-Google%20Colab-yellow)
---

## Overview

This module covers end-to-end Natural Language Processing (NLP) pipelines built on Apache Spark and the Spark NLP library. The three assignments span the core NLP task categories: sequence labeling (NER), unsupervised topic discovery (LDA), and supervised text classification (fake news detection). All work runs in Google Colab with PySpark and Spark NLP.

**Tools & Libraries:** PySpark · Spark NLP · BERT · Universal Sentence Encoder · Spark MLlib · LDA · Google Colab · CoNLL 2003 · John Snow Labs pretrained models

---

## Assignments

| # | Assignment | Techniques | Notebook |
|---|-----------|-----------|---------|
| 01 | [Named Entity Recognition with BERT](./01-ner/README.md) | BERT embeddings, NerDLApproach, CoNLL 2003, PretrainedPipeline | `EX5-1_rrata.ipynb`, `EX5-1_Q10_rrata.ipynb` |
| 02 | [Topic Modeling with LDA](./02-topic-modeling/README.md) | Spark NLP preprocessing, CountVectorizer, LDA, hyperparameter tuning | `BA_Ex5-2_rrata.ipynb` |
| 03 | [Fake News Detection](./03-fake-news/README.md) | Universal Sentence Encoder, pretrained classifiers, tweet & news datasets | `BA_Ex5-3_rrata.ipynb` |

---

## Key Concepts

**Spark NLP Pipeline** — A sequence of annotators that transform raw text through document assembly, tokenization, normalization, stopword removal, stemming, and embedding before reaching a model. All stages are chained in a `Pipeline` object and fit/transformed in a single pass.

**BERT Embeddings** — Contextual token representations from a pretrained transformer. Unlike static embeddings, BERT produces different vectors for the same word depending on surrounding context, which significantly improves NER and classification accuracy.

**LDA (Latent Dirichlet Allocation)** — An unsupervised topic modeling algorithm that discovers latent themes in a corpus by identifying word co-occurrence patterns. Spark MLlib's distributed LDA implementation scales to billions of documents.

**Universal Sentence Encoder (USE)** — A pretrained sentence-level embedding model that encodes full sentences into fixed-length vectors capturing semantic meaning, used as features for downstream classification.

**IOB2 Tagging** — The standard labeling scheme for NER: `B-` marks the beginning of an entity span, `I-` marks continuation, and `O` marks non-entity tokens. Entity types include `PER`, `ORG`, `LOC`, and `MISC`.

---

## Environment

All notebooks run in **Google Colab**. Packages must be reinstalled after each session disconnect.

```python
# Standard setup across all notebooks
! apt-get install -y openjdk-8-jdk-headless -qq > /dev/null
! pip install --ignore-installed pyspark==3.5.1
! pip install --ignore-installed spark-nlp==5.3.3
```

> Assignment 03 (fake/real news dataset) required `spark-nlp==5.4.0` and `pyspark==3.5.0` due to pretrained model compatibility.

---

## Relevance to Healthcare Data Engineering

The NLP patterns demonstrated here map directly to clinical text analytics. NER pipelines extract medications, diagnoses, and procedures from unstructured clinical notes — tools like BioBERT and ClinicalBERT apply the same BERT-based architecture to EHR data. LDA topic modeling is used to surface themes across large collections of radiology reports or patient feedback. Text classification pipelines like the fake news detector parallel clinical decision support tools that flag anomalous or high-risk documents in claims and prior authorization workflows.
