import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import re
import os

# === CONFIGURACIÓN ===
CSV_PATH = "/Users/matiasgodoy/Universidad/2025-1/Patos/bitcoin_2017_to_2023.csv"
SIM_FILE = "/Users/matiasgodoy/Universidad/2025-1/Patos/salida_cambios.txt"
OUTPUT_DIR = "plots_similares"
WINDOW_SIZE = 360  # 6 horas

# Crear carpeta de salida si no existe
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === CARGAR CSV ORIGINAL ===
print("[INFO] Cargando datos del CSV original...")
df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])
df = df[["timestamp", "close"]].dropna().sort_values("timestamp").reset_index(drop=True)
df.set_index("timestamp", inplace=True)

# === EXTRAER PARES DESDE EL ARCHIVO DE SIMILITUDES ===
print("[INFO] Leyendo archivo de similitudes...")
pattern = r"Ventana ([\d\-: ]+) es similar a ([\d\-: ]+) con similitud ([0-9.]+)"
pairs = []

with open(SIM_FILE, "r") as f:
    for line in f:
        match = re.match(pattern, line.strip())
        if match:
            fecha1, fecha2, sim = match.groups()
            t1 = pd.to_datetime(fecha1)
            t2 = pd.to_datetime(fecha2)
            sim = float(sim)
            pairs.append((t1, t2, sim))

print(f"[INFO] Total de pares encontrados: {len(pairs)}")

# === GENERAR GRÁFICOS ===
for i, (t1, t2, sim) in enumerate(pairs, 1):
    w1 = df.loc[t1 : t1 + timedelta(minutes=WINDOW_SIZE - 1)].reset_index()
    w2 = df.loc[t2 : t2 + timedelta(minutes=WINDOW_SIZE - 1)].reset_index()

    if len(w1) < WINDOW_SIZE or len(w2) < WINDOW_SIZE:
        print(f"[WARN] Ventana incompleta en {t1} o {t2}, se omite.")
        continue

    plt.figure(figsize=(10, 5))
    plt.plot(range(WINDOW_SIZE), w1["close"], label=f"{t1.strftime('%Y-%m-%d %H:%M')}")
    plt.plot(range(WINDOW_SIZE), w2["close"], label=f"{t2.strftime('%Y-%m-%d %H:%M')}")
    plt.title(f"Similitud: {sim:.4f}")
    plt.xlabel("Minutos desde el inicio de la ventana")
    plt.ylabel("Precio de cierre (close)")
    plt.legend()
    plt.tight_layout()

    fname = f"plot_{i:02d}_{t1.strftime('%Y%m%d_%H%M')}_{t2.strftime('%Y%m%d_%H%M')}.png"
    fpath = os.path.join(OUTPUT_DIR, fname)
    plt.savefig(fpath)
    plt.close()
    print(f"[INFO] Guardado: {fpath}")

print("\n✅ Todos los gráficos fueron generados.")
