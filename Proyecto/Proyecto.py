import sys
import math
import time
import statistics
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, concat_ws, lit, round as spark_round

# === PARÁMETROS ===
CSV_PATH = "hdfs:///data/2025/uhadoop/grupodematigodoy/bitcoin_2017_to_2023.csv"
OUTPUT_DIR = "file:///data/2025/uhadoop/grupodematigodoy/salida_cambios"
WINDOW_SIZE = 360  # 6 horas
BLOCK_SIZE = 10    # 10 minutos
STEP_SIZE = 60     # solo para modo 2 (desplazamiento de 1 hora)
SIMILARITY_THRESHOLD = 0.85

# === SIMILITUD DEL COSENO ===
def cosine_similarity(v1, v2):
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    if norm1 == 0.0 or norm2 == 0.0:
        return 0.0
    return dot / (norm1 * norm2)

def compute_similarity(pair):
    (idx_a, (ts_a, vec_a)), (idx_b, (ts_b, vec_b)) = pair
    sim = cosine_similarity(vec_a, vec_b)
    return (ts_a, ts_b, sim)

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"0", "1"}:
        print("Uso: spark-submit script.py <modo>")
        print("Modo 0: comparar todas las ventanas entre sí (como antes)")
        print("Modo 1: comparar última ventana contra desplazamientos cada 60 min")
        sys.exit(1)

    mode = int(sys.argv[1])

    spark = SparkSession.builder.appName("BitcoinSimilarityModes").getOrCreate()
    sc = spark.sparkContext

    print("[INFO] Leyendo CSV desde HDFS...")
    df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)
    df = df.filter(col("close").isNotNull()).orderBy(to_timestamp(col("timestamp")))
    data_list = df.select("timestamp", "close").rdd.map(lambda row: (row[0], float(row[1]))).collect()

    def build_window_vec(start_idx):
        block = data_list[start_idx:start_idx+WINDOW_SIZE]
        if len(block) < WINDOW_SIZE:
            return None
        raw_values = [x[1] for x in block]
        means = [statistics.mean(raw_values[i:i+BLOCK_SIZE])
                 for i in range(0, WINDOW_SIZE, BLOCK_SIZE)]
        changes = [(means[k+1] - means[k]) / means[k] for k in range(len(means)-1)]
        return (block[0][0], changes)

    if mode == 0:
        print("[INFO] Modo 0: todas las ventanas comparadas entre sí")
        windows = []
        for i in range(0, len(data_list), WINDOW_SIZE):
            w = build_window_vec(i)
            if w:
                windows.append(w)

        rdd = sc.parallelize(windows).zipWithIndex().map(lambda x: (x[1], x[0]))
        joined = rdd.cartesian(rdd).filter(lambda x: x[0][0] > x[1][0])
        similarities = joined.map(compute_similarity)
        similar_filtered = similarities.filter(lambda x: x[2] >= SIMILARITY_THRESHOLD)

    else:
        print("[INFO] Modo 1: última ventana vs ventanas desplazadas cada 60 minutos")
        last_window = build_window_vec(len(data_list) - WINDOW_SIZE)
        if last_window is None:
            print("[ERROR] No se pudo construir la última ventana")
            sys.exit(1)

        base_vec = last_window[1]
        target_windows = []
        for i in range(0, len(data_list) - WINDOW_SIZE, STEP_SIZE):
            w = build_window_vec(i)
            if w:
                target_windows.append(w)

        print(f"[INFO] Ventanas objetivo: {len(target_windows)}")
        rdd = sc.parallelize(target_windows)
        base_broadcast = sc.broadcast(base_vec)

        similarities = rdd.map(lambda x: (last_window[0], x[0], cosine_similarity(base_broadcast.value, x[1])))
        similar_filtered = similarities.sortBy(lambda x: -x[2]).zipWithIndex().filter(lambda x: x[1] < 10).map(lambda x: x[0])

    similar_df = similar_filtered.map(lambda x: (x[0], x[1], x[2])).toDF(["start_time_a", "start_time_b", "similarity"])

    similar_df = similar_df.withColumn(
        "result",
        concat_ws(" ",
            lit("Ventana"), col("start_time_a"),
            lit("es similar a"), col("start_time_b"),
            lit("con similitud:"), spark_round(col("similarity"), 4))
    ).select("result")

    print("[INFO] Guardando resultados en carpeta local...")
    similar_df.write.mode("overwrite").text(OUTPUT_DIR)
    print(f"[INFO] Resultados guardados en {OUTPUT_DIR}")

    spark.stop()
