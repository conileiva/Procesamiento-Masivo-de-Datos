import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta
import re
import os
import glob

# === CONFIGURACIÓN ===
CSV_PATH = "/Users/matiasgodoy/Universidad/2025-1/Patos/bitcoin_2017_to_2023.csv"
SIM_FOLDER = "/Users/matiasgodoy/Universidad/2025-1/Patos/Proyecto/output"  # Carpeta con archivos .txt de similitudes
OUTPUT_DIR = "plots_similares0"
WINDOW_SIZE = 360  # 6 horas en minutos

# Crear carpeta de salida si no existe
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === CARGAR CSV ORIGINAL ===
print("[INFO] Cargando datos del CSV original...")
df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])
df = df[["timestamp", "close"]].dropna().sort_values("timestamp").reset_index(drop=True)
df.set_index("timestamp", inplace=True)

# === FUNCIONES ===
def extraer_pares_desde_archivo(filepath):
    pattern = r"Ventana ([\d\-: ]+) es similar a ([\d\-: ]+) con similitud: ([0-9.]+)"
    pares = []
    with open(filepath, "r") as f:
        for line in f:
            match = re.match(pattern, line.strip())
            if match:
                fecha1, fecha2, sim = match.groups()
                t1 = pd.to_datetime(fecha1)
                t2 = pd.to_datetime(fecha2)
                sim = float(sim)
                pares.append((t1, t2, sim))
    return pares

# === EXTRAER TODOS LOS PARES DE TODOS LOS TXT ===
print("[INFO] Leyendo todos los archivos de similitudes en carpeta...")
all_pairs = []

txt_files = glob.glob(os.path.join(SIM_FOLDER, "*.txt"))
print(f"[INFO] Archivos encontrados: {len(txt_files)}")

for filepath in txt_files:
    pares = extraer_pares_desde_archivo(filepath)
    print(f"[INFO] {os.path.basename(filepath)} → pares: {len(pares)}")
    all_pairs.extend(pares)

print(f"[INFO] Total de pares combinados: {len(all_pairs)}")

# === GENERAR GRÁFICOS ===
print("[INFO] Generando gráficos...")

for i, (t1, t2, sim) in enumerate(all_pairs, 1):
    w1 = df.loc[t1 : t1 + timedelta(minutes=WINDOW_SIZE - 1)].reset_index()
    w2 = df.loc[t2 : t2 + timedelta(minutes=WINDOW_SIZE - 1)].reset_index()

    if len(w1) < WINDOW_SIZE or len(w2) < WINDOW_SIZE:
        print(f"[WARN] Ventana incompleta en {t1} o {t2}, se omite.")
        continue

    plt.figure(figsize=(10, 5))
    plt.plot(range(WINDOW_SIZE), w1["close"], label=f"{t1.strftime('%Y-%m-%d %H:%M')}")
    plt.plot(range(WINDOW_SIZE), w2["close"], label=f"{t2.strftime('%Y-%m-%d %H:%M')}")
    plt.title(f"Similitud: {sim:.4f}")
    plt.xlabel("Minutos desde inicio ventana")
    plt.ylabel("Precio de cierre (close)")
    plt.legend()
    plt.tight_layout()

    fname = f"plot_{i:04d}_{t1.strftime('%Y%m%d_%H%M')}_{t2.strftime('%Y%m%d_%H%M')}.png"
    fpath = os.path.join(OUTPUT_DIR, fname)
    plt.savefig(fpath)
    plt.close()

    if i % 10 == 0:
        print(f"[INFO] Generados {i} gráficos...")

print(f"\n✅ Todos los gráficos fueron generados en la carpeta '{OUTPUT_DIR}'.")
