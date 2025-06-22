from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
import math
import time
import statistics

# Parámetros
CSV_PATH = "file:///data/2025/uhadoop/grupodematigodoy/bitcoin_2017_to_2023.csv"
WINDOW_SIZE = 360  # 6 horas * 60 minutos
BLOCK_SIZE = 10    # 10 minutos
SIMILARITY_THRESHOLD = 0.85

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
    spark = SparkSession.builder.appName("BitcoinWindowSimilarityChangeStd").getOrCreate()
    sc = spark.sparkContext

    print("[INFO] Leyendo CSV...")
    df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)
    df = df.filter(col("close").isNotNull()).orderBy(to_timestamp(col("timestamp")))
    total_rows = df.count()
    print(f"[INFO] Total filas: {total_rows}")

    # Usar primera mitad de datos sin muestreo aleatorio
    half_count = total_rows
    df_sample = df.limit(half_count)
    print(f"[INFO] Usando primera mitad: {half_count} filas")

    print("[INFO] Colectando datos localmente...")
    data_list = df_sample.select("timestamp", "close").rdd.map(lambda row: (row[0], float(row[1]))).collect()

    print("[INFO] Creando ventanas fijas y calculando vectores de cambios y desviaciones...")
    windows_change = []
    windows_change_std = []

    for i in range(0, len(data_list), WINDOW_SIZE):
        block = data_list[i:i+WINDOW_SIZE]
        if len(block) == WINDOW_SIZE:
            timestamps = [x[0] for x in block]
            raw_values = [x[1] for x in block]

            means = []
            stds = []
            for j in range(0, WINDOW_SIZE, BLOCK_SIZE):
                subblock = raw_values[j:j+BLOCK_SIZE]
                if len(subblock) == BLOCK_SIZE:
                    means.append(statistics.mean(subblock))
                    stds.append(statistics.stdev(subblock))

            changes = [
                (means[k+1] - means[k]) / means[k]
                for k in range(len(means)-1)
            ]

            # Vector solo con cambios porcentuales
            windows_change.append((timestamps[0], changes))

            # Vector combinado cambios + std
            combined_vec = changes + stds
            windows_change_std.append((timestamps[0], combined_vec))

    print(f"[INFO] Total ventanas creadas: {len(windows_change)}")

    # Paralelizar y asignar índice
    rdd_change = sc.parallelize(windows_change).zipWithIndex().map(lambda x: (x[1], x[0]))
    rdd_change_std = sc.parallelize(windows_change_std).zipWithIndex().map(lambda x: (x[1], x[0]))

    def process_similarity(rdd, output_file):
        print(f"[INFO] Calculando similitudes para {output_file} ...")
        start_time = time.time()

        joined = rdd.cartesian(rdd).filter(lambda x: x[0][0] > x[1][0])
        all_similarities = joined.map(compute_similarity).cache()

        top_10 = all_similarities.takeOrdered(10, key=lambda x: -x[2])
        print(f"\n📊 Top 10 similitudes ({output_file}):")
        for s in top_10:
            print(f"Ventana {s[0]} ↔ {s[1]} → Similitud: {s[2]:.4f}")

        similar = all_similarities.filter(lambda x: x[2] >= SIMILARITY_THRESHOLD).collect()
        elapsed = time.time() - start_time
        print(f"[INFO] Similitudes calculadas en {elapsed:.2f} segundos.")
        print(f"[INFO] Ventanas similares encontradas sobre umbral ({SIMILARITY_THRESHOLD}): {len(similar)}")

        with open(output_file, "w") as f:
            for s in similar:
                f.write(f"Ventana {s[0]} es similar a {s[1]} con similitud {s[2]:.4f}\n")
        print(f"[INFO] Resultados guardados en {output_file}")

    # Ejecutar para ambos
    process_similarity(rdd_change, "/data/2025/uhadoop/grupodematigodoy/salida_cambios.txt")
    process_similarity(rdd_change_std, "/data/2025/uhadoop/grupodematigodoy/salida_cambios_std.txt")

    spark.stop()
