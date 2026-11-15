import json
import os
import socket
import sys
import time
from collections import Counter
from pyspark.sql import SparkSession

MASTER_IP = os.getenv("SPARK_DRIVER_HOST", "192.168.137.1")

spark = (
    SparkSession.builder
    .appName("RemoteWorkerPythonProbe")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", MASTER_IP)
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.port", "44575")
    .config("spark.driver.blockManager.port", "44576")
    .config("spark.ui.bindAddress", "0.0.0.0")
    .config("spark.ui.port", "4040")
    .config("spark.network.timeout", "240s")
    .config("spark.executor.heartbeatInterval", "30s")
    .config("spark.rpc.askTimeout", "240s")
    .config("spark.pyspark.python", os.getenv("SPARK_PYSPARK_PYTHON", "python3.10"))
    .config("spark.pyspark.driver.python", os.getenv("SPARK_PYSPARK_DRIVER_PYTHON", "/usr/bin/python3"))
    .config("spark.executorEnv.PYSPARK_PYTHON", os.getenv("SPARK_PYSPARK_PYTHON", "python3.10"))
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.blockManager.port", "7100")
    .config("spark.executor.cores", "2")
    .config("spark.executor.instances", "1")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.memoryOverhead", "128m")
    .config("spark.cores.max", os.getenv("SPARK_CORES_MAX", "2"))
    .config("spark.port.maxRetries", "20")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

def identify_host(rows):
    values = list(rows)
    time.sleep(0.15)
    yield {
        "hostname": socket.gethostname(),
        "ip": socket.gethostbyname(socket.gethostname()),
        "python_version": sys.version.split()[0],
        "python_executable": sys.executable,
        "partition_records": len(values),
    }

try:
    results = sc.parallelize(range(200), 20).mapPartitions(identify_host).collect()
    counts = Counter((row["hostname"], row["ip"], row["python_version"], row["python_executable"]) for row in results)
    report = [
        {"hostname": host, "ip": ip, "python_version": version,
         "python_executable": executable, "partitions_executed": count}
        for (host, ip, version, executable), count in sorted(counts.items())
    ]
    print("REMOTE_WORKER_PYTHON_PROBE=" + json.dumps(report), flush=True)
    print("TASK_PARTITIONS_COMPLETED=" + str(len(results)), flush=True)
    print("PROBE_RESULT=SUCCESS", flush=True)
finally:
    spark.stop()
