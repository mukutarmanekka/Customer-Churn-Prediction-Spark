import json, os, shutil
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(os.getenv("OUTPUT_DIR", str(ROOT / "artifacts")))
HDFS_MASTER_HOST = os.getenv("HDFS_MASTER_HOST", os.getenv("SPARK_DRIVER_HOST", "192.168.137.1")).strip()
HDFS_NAMENODE_URI = os.getenv("HDFS_NAMENODE_URI", f"hdfs://{HDFS_MASTER_HOST}:9000").rstrip("/")
LABELS = os.getenv("LABELS_PATH", f"{HDFS_NAMENODE_URI}/kkbox/input/train.csv")
TRANSACTIONS = os.getenv("TRANSACTIONS_PATH", f"{HDFS_NAMENODE_URI}/kkbox/input/transactions.csv")
MEMBERS = os.getenv("MEMBERS_PATH", f"{HDFS_NAMENODE_URI}/kkbox/input/members_v3.csv")
HDFS_OUTPUT_DIR = os.getenv("HDFS_OUTPUT_DIR", f"{HDFS_NAMENODE_URI}/kkbox/output")
CUTOFF = "2017-02-28"

def clear(path):
    if path.exists():
        shutil.rmtree(path)
    path.parent.mkdir(parents=True, exist_ok=True)

def main():
    executor_python = os.getenv("SPARK_PYSPARK_PYTHON", "python3.10")
    driver_python = os.getenv("SPARK_PYSPARK_DRIVER_PYTHON", "/usr/bin/python3")
    spark = (SparkSession.builder.appName("KKBoxCustomerChurnPrediction")
             .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "192.168.137.1"))
             .config("spark.driver.bindAddress", "0.0.0.0")
             .config("spark.driver.port", "44575")
             .config("spark.driver.blockManager.port", "44576")
             .config("spark.ui.bindAddress", "0.0.0.0")
             .config("spark.ui.port", "4040")
             .config("spark.network.timeout", "240s")
             .config("spark.executor.heartbeatInterval", "30s")
             .config("spark.rpc.askTimeout", "240s")
             .config("spark.pyspark.python", executor_python)
             .config("spark.pyspark.driver.python", driver_python)
             .config("spark.executorEnv.PYSPARK_PYTHON", executor_python)
             .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
             .config("spark.blockManager.port", "7100")
             .config("spark.port.maxRetries", "20")
             .config("spark.cores.max", os.getenv("SPARK_CORES_MAX", "32"))
             .config("spark.executor.cores", "1")
             .config("spark.sql.shuffle.partitions", "48")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    labels = (spark.read.option("header", True).option("inferSchema", True).csv(LABELS)
              .select("msno", F.col("is_churn").cast("double").alias("label"))
              .dropna(subset=["msno", "label"]).dropDuplicates(["msno"]))

    # Restrict the 21.5M-row transaction table to the labeled cohort before aggregation.
    tx = (spark.read.option("header", True).option("inferSchema", True).csv(TRANSACTIONS)
          .join(labels.select("msno"), "msno", "left_semi")
          .withColumn("transaction_date", F.to_date(F.col("transaction_date").cast("string"), "yyyyMMdd"))
          .withColumn("membership_expire_date", F.to_date(F.col("membership_expire_date").cast("string"), "yyyyMMdd"))
          .filter(F.col("transaction_date").isNotNull() & (F.col("transaction_date") <= F.lit(CUTOFF)))
          .withColumn("payment_plan_days", F.col("payment_plan_days").cast("double"))
          .withColumn("plan_list_price", F.col("plan_list_price").cast("double"))
          .withColumn("actual_amount_paid", F.col("actual_amount_paid").cast("double"))
          .withColumn("is_auto_renew", F.col("is_auto_renew").cast("double"))
          .withColumn("is_cancel", F.col("is_cancel").cast("double"))
          .withColumn("payment_method_id", F.col("payment_method_id").cast("double")))

    tx = tx.withColumn("days_from_cutoff", F.datediff(F.lit(CUTOFF), F.col("transaction_date")))
    tx = tx.withColumn("recent_30", (F.col("days_from_cutoff") <= 30).cast("int"))
    tx = tx.withColumn("recent_90", (F.col("days_from_cutoff") <= 90).cast("int"))
    tx = tx.withColumn("recent_180", (F.col("days_from_cutoff") <= 180).cast("int"))

    agg = tx.groupBy("msno").agg(
        F.count("*").alias("transaction_count"),
        F.sum("actual_amount_paid").alias("lifetime_amount_paid"),
        F.avg("actual_amount_paid").alias("avg_amount_paid"),
        F.avg("plan_list_price").alias("avg_plan_price"),
        F.avg("payment_plan_days").alias("avg_plan_days"),
        F.avg("is_auto_renew").alias("auto_renew_rate"),
        F.avg("is_cancel").alias("cancel_rate"),
        F.min("days_from_cutoff").alias("days_since_last_transaction"),
        F.sum("recent_30").alias("transactions_last_30d"),
        F.sum("recent_90").alias("transactions_last_90d"),
        F.sum("recent_180").alias("transactions_last_180d"),
        F.sum(F.when(F.col("recent_90") == 1, F.col("actual_amount_paid")).otherwise(0)).alias("amount_last_90d"),
        F.max_by("payment_method_id", "transaction_date").alias("last_payment_method"),
        F.max_by("is_auto_renew", "transaction_date").alias("last_auto_renew"),
        F.max_by("is_cancel", "transaction_date").alias("last_cancel"),
        F.max("membership_expire_date").alias("latest_expiry_date")
    )

    members = (spark.read.option("header", True).option("inferSchema", True).csv(MEMBERS)
               .select("msno", "city", "bd", "gender", "registered_via", "registration_init_time")
               .dropDuplicates(["msno"])
               .withColumn("registration_date", F.to_date(F.col("registration_init_time").cast("string"), "yyyyMMdd"))
               .withColumn("registration_days", F.datediff(F.lit(CUTOFF), F.col("registration_date")))
               .withColumn("bd", F.col("bd").cast("double"))
               .withColumn("bd", F.when((F.col("bd") >= 10) & (F.col("bd") <= 100), F.col("bd")).otherwise(None))
               .withColumn("city", F.col("city").cast("double"))
               .withColumn("registered_via", F.col("registered_via").cast("double"))
               .withColumn("gender_code", F.when(F.col("gender") == "male", 1.0).when(F.col("gender") == "female", 2.0).otherwise(0.0))
               .select("msno", "city", "bd", "registered_via", "gender_code", "registration_days"))

    data = (labels.join(agg, "msno", "left").join(members, "msno", "left")
            .fillna(0))
    # Stable hash split; the official competition labels do not expose a usable event timestamp.
    data = data.withColumn("bucket", F.pmod(F.xxhash64("msno"), F.lit(100)))
    data = data.withColumn("split", F.when(F.col("bucket") < 70, "train")
                           .when(F.col("bucket") < 85, "validation").otherwise("test")).cache()

    counts = {r["split"]: r["count"] for r in data.groupBy("split").count().collect()}
    train = data.filter("split='train'")
    val = data.filter("split='validation'")
    test = data.filter("split='test'")
    class_counts = {int(r["label"]): r["count"] for r in train.groupBy("label").count().collect()}
    if len(class_counts) != 2 or not counts.get("validation") or not counts.get("test"):
        raise ValueError(f"Invalid split/class distribution: {counts}, {class_counts}")

    total = sum(class_counts.values())
    weights = {0: total / (2 * class_counts[0]), 1: total / (2 * class_counts[1])}
    train = train.withColumn("class_weight", F.when(F.col("label") == 1, F.lit(weights[1])).otherwise(F.lit(weights[0])))

    features = ["transaction_count", "lifetime_amount_paid", "avg_amount_paid", "avg_plan_price",
                "avg_plan_days", "auto_renew_rate", "cancel_rate", "days_since_last_transaction",
                "transactions_last_30d", "transactions_last_90d", "transactions_last_180d",
                "amount_last_90d", "last_payment_method", "last_auto_renew", "last_cancel",
                "city", "bd", "registered_via", "gender_code", "registration_days"]
    assembler = VectorAssembler(inputCols=features, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=False, withStd=True)
    candidates = [
        ("LogisticRegression", LogisticRegression(featuresCol="features", labelCol="label",
          weightCol="class_weight", maxIter=80, regParam=0.05)),
        ("RandomForest", RandomForestClassifier(featuresCol="features", labelCol="label",
          weightCol="class_weight", numTrees=80, maxDepth=8, seed=42)),
        ("GradientBoostedTrees", GBTClassifier(featuresCol="features", labelCol="label",
          weightCol="class_weight", maxIter=35, maxDepth=5, stepSize=0.08, seed=42))
    ]
    pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
    roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    results, fitted = [], {}
    for name, estimator in candidates:
        model = Pipeline(stages=[assembler, scaler, estimator]).fit(train)
        validation = model.transform(val)
        results.append({"model": name, "validation_pr_auc": pr.evaluate(validation),
                        "validation_roc_auc": roc.evaluate(validation)})
        fitted[name] = model

    winner = max(results, key=lambda x: x["validation_pr_auc"])["model"]
    model = fitted[winner]
    val_scores = model.transform(val).withColumn("churn_probability", vector_to_array("probability")[1])
    val_rows = val_scores.select("label", "churn_probability").collect()
    threshold_rows = []
    for threshold in [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.60, 0.70]:
        tp = sum(r.label == 1 and r.churn_probability >= threshold for r in val_rows)
        fp = sum(r.label == 0 and r.churn_probability >= threshold for r in val_rows)
        fn = sum(r.label == 1 and r.churn_probability < threshold for r in val_rows)
        precision = tp / (tp + fp) if tp + fp else 0
        recall = tp / (tp + fn) if tp + fn else 0
        threshold_rows.append({"threshold": threshold, "precision": precision, "recall": recall,
                               "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0})
    best = max(threshold_rows, key=lambda x: x["f1"])
    threshold = best["threshold"]

    scored = model.transform(test).withColumn("churn_probability", vector_to_array("probability")[1])
    predictions = scored.withColumn("predicted_churn", (F.col("churn_probability") >= threshold).cast("int"))
    predictions = predictions.withColumn("churn_risk",
        F.when(F.col("churn_probability") >= 0.70, "High")
         .when(F.col("churn_probability") >= 0.35, "Medium").otherwise("Low"))

    matrix = {(int(r.label), int(r.predicted_churn)): r["count"]
              for r in predictions.groupBy("label", "predicted_churn").count().collect()}
    tn, fp, fn, tp = matrix.get((0, 0), 0), matrix.get((0, 1), 0), matrix.get((1, 0), 0), matrix.get((1, 1), 0)
    n = predictions.count()
    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    specificity = tn / (tn + fp) if tn + fp else 0
    metrics = {
        "dataset": "KKBox WSDM 2018 Churn Prediction Challenge",
        "objective": "Predict subscriber churn using pre-2017-02-28 subscription and member history",
        "label_definition": "Official is_churn target supplied by the competition",
        "feature_cutoff": CUTOFF,
        "training_rows": counts.get("train", 0), "validation_rows": counts.get("validation", 0),
        "test_rows": n, "label_distribution": {str(k): int(v) for k, v in class_counts.items()},
        "churn_rate": data.agg(F.avg("label")).first()[0],
        "validation_model_comparison": results, "selected_model": winner,
        "validation_threshold": threshold, "validation_threshold_metrics": best,
        "test_pr_auc": pr.evaluate(scored), "test_roc_auc": roc.evaluate(scored),
        "test_precision": precision, "test_recall": recall,
        "test_f1": 2 * precision * recall / (precision + recall) if precision + recall else 0,
        "test_specificity": specificity, "test_balanced_accuracy": (recall + specificity) / 2,
        "test_accuracy": (tn + tp) / n,
        "test_confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
        "features": features, "created_at": datetime.now(timezone.utc).isoformat()
    }
    OUT.mkdir(parents=True, exist_ok=True)
    hdfs_root = HDFS_OUTPUT_DIR.rstrip("/")
    hdfs_predictions = hdfs_root + "/predictions"
    hdfs_model = hdfs_root + "/model"
    predictions.select("msno", "churn_probability", "predicted_churn", "churn_risk",
                       "transaction_count", "days_since_last_transaction", "auto_renew_rate",
                       "cancel_rate", "city", "registered_via", "label").coalesce(1).write.mode("overwrite").option("header", True).csv(hdfs_predictions)
    model.write().overwrite().save(hdfs_model)
    (OUT / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps(metrics, indent=2))
    spark.stop()

if __name__ == "__main__":
    main()
