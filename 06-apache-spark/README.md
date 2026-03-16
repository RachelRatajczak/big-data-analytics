# 06 — Apache Spark
PySpark SQL analytics on COVID-19 data and ML pipelines for retweet prediction — window functions, logistic regression, and NLP feature engineering

![Spark](https://img.shields.io/badge/Apache-Spark-orange) ![PySpark](https://img.shields.io/badge/Language-PySpark-blue) ![ML](https://img.shields.io/badge/PySpark-ML-purple)

> Graduate coursework — University of Illinois Springfield, Data Analytics (CSC 534)

## Overview
Two Spark assignments. Assignment 1 covers PySpark DataFrame operations and Spark SQL analytics on COVID-19 worldwide data — filtering, group-by, ROLLUP, running totals, LAG delta calculations, and RANK() window functions. Assignment 2 covers Spark ML pipelines — logistic regression on dense vectors, text classification with Tokenizer/HashingTF, and a full NLP pipeline to predict COVID-19 tweet retweet counts.

## Datasets
| File | Description |
|------|-------------|
| `coronavirus-text-only-1000.txt` | 1,000 COVID-19 news articles |
| `COVID19-worldwide.csv` | Daily case/death counts by country |
| `TweetsCOV19-train.tsv` | COVID-19 tweets with retweet counts — training set |
| `TweetsCOV19-test.tsv` | Held-out tweets for prediction evaluation |

---

## Assignment 1 — Spark SQL and DataFrame analytics

### Text DataFrame
```python
textFile = spark.read.text("/user/rrata/COVID19/coronavirus-text-only-1000.txt")
textFile.count()
textFile.first()
textFile.show()
textFile.show(2)
textFile.show(10, False)
textFile.filter(textFile.value.contains("wear masks")).show(10, False)
```

### Load CSV — default schema (all strings)
```python
df = spark.read.option("header","true").csv("/user/data/CSC534BDA/COVID19/COVID19-worldwide.csv")
df.printSchema()
df.select("dateRep","cases","deaths","countriesAndTerritories") \
  .filter("countryterritoryCode == 'USA'").show()
```

### InferSchema and group-by
```python
df2 = spark.read.option("header","true").option("inferSchema","true") \
           .csv("/user/data/CSC534BDA/COVID19/COVID19-worldwide.csv")
df2.printSchema()  # cases: int, deaths: int, Cumulative: double

from pyspark.sql import functions as F
df2.groupBy("continentExp").agg(F.sum("cases"), F.sum("deaths")).show()
# America: 19,238,368 / 617,762  Asia: 12,699,673 / 227,092

df2.createOrReplaceTempView("covid19_stat")
spark.sql("SELECT continentExp, sum(cases), sum(deaths) FROM covid19_stat GROUP BY continentExp").show()
```

### Total cases per country and ROLLUP
```python
spark.sql("""SELECT countriesAndTerritories, SUM(cases) AS total_cases,
SUM(deaths) AS total_deaths FROM covid19_stat
GROUP BY countriesAndTerritories""").show(1000)

spark.sql("""SELECT ContinentExp, countriesAndTerritories,
SUM(cases) AS total_cases, SUM(deaths) AS total_deaths
FROM covid19_stat GROUP BY ROLLUP(ContinentExp, countriesAndTerritories)
ORDER BY total_cases DESC""").filter("ContinentExp == 'America'").show(1000)
# America: 19,238,368 · USA: 8,336,282 · Brazil: 5,298,772
```

### Running total
```python
df3.createOrReplaceTempView("covid19_stat_date")
spark.sql("""SELECT countryterritoryCode AS country, date, cases,
SUM(cases) OVER(PARTITION BY countryterritoryCode ORDER BY date) AS running_total
FROM covid19_stat_date""").filter("countryterritoryCode == 'USA'").show(1000)
```

### Severe death days — deaths > 800 (Assignment 1)
```python
df.select("dateRep","cases","deaths","countriesAndTerritories") \
  .filter("deaths > 800 AND countryterritoryCode == 'USA'").show()
# 20 days. Highest: 10/22/20 — 62,978 cases, 1,135 deaths
```

### Daily case delta with LAG() (Assignment 3)
```python
from pyspark.sql.functions import col, to_date
df4 = spark.read.option("header","true").option("inferSchema","true") \
           .csv("/user/data/CSC534BDA/COVID19/COVID19-worldwide.csv")
df4 = df4.withColumn("Date", to_date(col('dateRep'), 'MM/dd/yy'))
df4.createOrReplaceTempView("covid19_stat")

df4_delta = spark.sql("""
SELECT countryterritoryCode AS country, Date AS date, cases,
    (cases - LAG(cases, 1, NULL) OVER (
        PARTITION BY countriesAndTerritories ORDER BY Date
    )) AS cases_delta
FROM covid19_stat ORDER BY Date
""").filter("country == 'USA'")

df4_delta.show(5)
df4_delta.orderBy(col("date").desc()).show(5)
# 10/22/20: cases=62978, delta=+4429
# Negative deltas reflect source data reporting corrections
```

### Top country per day with RANK() (Assignment 4)
```python
top_countries_daily = spark.sql("""
SELECT Date AS date, countryterritoryCode AS country, cases
FROM (
    SELECT Date, countryterritoryCode, cases,
           RANK() OVER (PARTITION BY Date ORDER BY cases DESC) AS rank
    FROM covid19_stat
    WHERE Date BETWEEN '2020-10-11' AND '2020-10-18'
) ranked
WHERE rank = 1
ORDER BY date ASC
""")
```

---

## Assignment 2 — ML pipeline in Apache Spark

### ML pipeline concepts
| Concept | Role |
|---------|------|
| DataFrame | ML dataset — label, features, prediction columns |
| Transformer | Converts DataFrame to DataFrame — Tokenizer, HashingTF |
| Estimator | Fits on data to produce Transformer — LogisticRegression |
| Pipeline | Chains Transformers and Estimators in order |
| ParamMap | Runtime parameter overrides at fit time |

### Logistic regression on dense vectors
```python
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression

training = spark.createDataFrame([
    (1.0, Vectors.dense([0.0, 1.1, 0.1])),
    (0.0, Vectors.dense([2.0, 1.0, -1.0])),
    (0.0, Vectors.dense([2.0, 1.3, 1.0])),
    (1.0, Vectors.dense([0.0, 1.2, -0.5]))
], ["label", "features"])

lr = LogisticRegression(maxIter=10, regParam=0.01)
print("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
model1 = lr.fit(training)
```

### ParamMap parameter specification
```python
paramMap = {lr.maxIter: 20}
paramMap[lr.maxIter] = 30
paramMap.update({lr.regParam: 0.1, lr.threshold: 0.55})
paramMap2 = {lr.probabilityCol: "myProbability"}
paramMapCombined = paramMap.copy()
paramMapCombined.update(paramMap2)

model2 = lr.fit(training, paramMapCombined)
print(model2.extractParamMap())
# maxIter=30, regParam=0.1, threshold=0.55, probabilityCol='myProbability'
```

### Predictions
```python
test = spark.createDataFrame([
    (1.0, Vectors.dense([-1.0, 1.5, 1.3])),
    (0.0, Vectors.dense([3.0, 2.0, -0.1])),
    (1.0, Vectors.dense([0.0, 2.2, -1.5]))
], ["label", "features"])

prediction = model2.transform(test)
result = prediction.select("features", "label", "myProbability", "prediction").collect()
for row in result:
    print("features=%s, label=%s -> prob=%s, prediction=%s"
          % (row.features, row.label, row.myProbability, row.prediction))
# features=[-1.0,1.5,1.3], label=1.0 -> prob=[0.057, 0.943], prediction=1.0
# features=[3.0,2.0,-0.1], label=0.0 -> prob=[0.924, 0.076], prediction=0.0
# features=[0.0,2.2,-1.5], label=1.0 -> prob=[0.110, 0.890], prediction=1.0
```

### Text classification pipeline
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, Tokenizer

training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0), (1, "b d", 0.0),
    (2, "spark f g h", 1.0),     (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF  = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr         = LogisticRegression(maxIter=10, regParam=0.001)
pipeline   = Pipeline(stages=[tokenizer, hashingTF, lr])
model      = pipeline.fit(training)

test = spark.createDataFrame([
    (4, "spark i j k"), (5, "l m n"),
    (6, "spark hadoop spark"), (7, "apache hadoop")
], ["id", "text"])

prediction = model.transform(test)
selected = prediction.select("id", "text", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row
    print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))
```

### Retweet prediction: TweetsCOV19 (Assignment 5)
```python
train_tsv_file = "/user/data/CSC534BDA/COVID19-Retweet/TweetsCOV19-train.tsv"
test_tsv_file  = "/user/data/CSC534BDA/COVID19-Retweet/TweetsCOV19-test.tsv"

columns = ["TweetId","Username","Timestamp","Followers","Friends",
           "Retweets","Favorites","Entities","Sentiment","Mentions","Hashtags","URLs"]

train_df = spark.read.option("header","false").option("sep","\t").csv(train_tsv_file).toDF(*columns)
test_df  = spark.read.option("header","false").option("sep","\t").csv(test_tsv_file).toDF(*columns)

from pyspark.sql.functions import col, when, expr

def preprocess_entities(df):
    return df.withColumn(
        "Entities_clean",
        when(col("Entities").isNull() | (col("Entities") == "null;"), "")
        .otherwise(col("Entities"))
    ).withColumn(
        "Entities_text",
        expr("""concat_ws(' ', transform(
                    filter(split(Entities_clean, ';'), x -> x != ''),
                    x -> split(x, ':')[1]))""")
    )

train_df = preprocess_entities(train_df)
test_df  = preprocess_entities(test_df)

train_df = train_df.withColumn("Retweets_binary", when(col("Retweets") > 0, 1).otherwise(0))
test_df  = test_df.withColumn("Retweets_binary",  when(col("Retweets") > 0, 1).otherwise(0))

tokenizer = Tokenizer(inputCol="Entities_clean", outputCol="words")
hashingTF  = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(labelCol="Retweets_binary", featuresCol="features",
                        maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
model = pipeline.fit(train_df)

prediction = model.transform(test_df)
selected = prediction.select("TweetId","Entities_text","Retweets_binary","probability","prediction")
for row in selected.collect():
    id, text, retweet_bin, prob, prediction = row
    print("(%s, %s) --> actual=%s, prob=%s, prediction=%s"
          % (id, text, retweet_bin, str(prob), str(prediction)))
# ~94% accuracy on 30-row test set
```

---

## Key concepts demonstrated
- Spark DataFrames are distributed — all operations run in parallel across the cluster
- InferSchema automatically detects column types at read time
- Spark SQL and DataFrame API are interchangeable for the same queries
- Window functions (SUM OVER PARTITION, LAG, RANK) enable time-series analysis without self-joins
- LAG() computes row-over-row deltas — applicable to clinical trend analysis
- RANK() OVER PARTITION identifies top performers per time period — useful for outbreak surveillance
- Spark ML pipelines chain preprocessing and model training into a single reusable object
- HashingTF converts text tokens to fixed-length vectors — no vocabulary needed
- Binary classification reframes retweet count regression as a tractable classification problem
- NLP pipeline pattern applies to clinical text classification (e.g. ICD code prediction from notes)
