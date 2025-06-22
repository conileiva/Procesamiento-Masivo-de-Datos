import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Ruta al CSV local
CSV_PATH = "/Users/matiasgodoy/Universidad/2025-1/Patos/bitcoin_2017_to_2023.csv"  # <-- modifica aquí
WINDOW_SIZE = 360  # minutos

# Lista de pares similares: (timestamp_1, timestamp_2, similitud)
similitudes = [
    ("2018-08-27 19:27:00", "2018-05-16 06:05:00", 0.8508),
    ("2018-06-29 17:50:00", "2018-06-16 12:05:00", 0.8519),
    ("2018-10-10 19:27:00", "2018-06-09 18:05:00", 0.9100),
    ("2018-10-26 04:57:00", "2018-08-11 13:27:00", 0.8835),
    ("2019-01-06 16:40:00", "2019-01-01 22:40:00", 0.8772),
    ("2019-01-10 10:40:00", "2019-01-05 16:40:00", 0.8648),
    ("2019-01-13 10:40:00", "2019-01-10 10:40:00", 0.8649),
    ("2019-04-01 22:40:00", "2018-09-19 13:27:00", 0.8934),
    ("2019-10-13 17:41:00", "2018-05-15 12:05:00", 0.8515),
    ("2019-10-13 17:41:00", "2018-08-22 13:27:00", 0.8789),
    ("2019-11-10 11:41:00", "2018-06-29 17:50:00", 0.8682),
    ("2019-07-08 03:41:00", "2019-01-19 04:40:00", 0.9205),
    ("2019-10-17 23:41:00", "2018-12-29 17:57:00", 0.9054),
    ("2019-09-08 05:41:00", "2019-04-25 16:40:00", 0.8764),
    ("2020-03-12 05:22:00", "2019-03-08 16:40:00", 0.8936),
    ("2020-03-12 05:22:00", "2019-04-25 16:40:00", 0.8514),
    ("2019-12-16 16:04:00", "2019-10-13 17:41:00", 0.8678),
    ("2020-01-03 02:20:00", "2019-09-19 17:41:00", 0.8692),
    ("2020-02-19 21:14:00", "2019-07-09 09:41:00", 0.9202),
    ("2020-02-19 21:14:00", "2019-08-28 17:41:00", 0.8734),
    ("2020-08-10 05:22:00", "2018-05-10 12:05:00", 0.8516),
    ("2020-07-26 05:22:00", "2018-06-29 17:50:00", 0.9213),
    ("2020-04-04 11:22:00", "2019-09-16 11:41:00", 0.8512),
    ("2020-06-08 19:52:00", "2019-10-15 11:41:00", 0.8880),
    ("2020-06-27 13:52:00", "2019-10-17 23:41:00", 0.8539),
    ("2020-07-26 05:22:00", "2019-11-10 11:41:00", 0.8995),
    ("2020-06-02 13:52:00", "2019-11-21 14:04:00", 0.8574),
    ("2020-08-01 23:22:00", "2020-03-12 05:22:00", 0.9208)
]

# Cargar CSV
print("[INFO] Cargando CSV...")
df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])
df = df[["timestamp", "close"]].dropna().sort_values("timestamp").reset_index(drop=True)

# Convertir timestamps a índice para rápido acceso
df.set_index("timestamp", inplace=True)

# Crear plots
print("[INFO] Generando gráficos...")
for i, (t1_str, t2_str, sim) in enumerate(similitudes):
    t1 = pd.to_datetime(t1_str)
    t2 = pd.to_datetime(t2_str)

    # Definir ventanas
    window1 = df.loc[t1 : t1 + timedelta(minutes=WINDOW_SIZE - 1)].reset_index()
    window2 = df.loc[t2 : t2 + timedelta(minutes=WINDOW_SIZE - 1)].reset_index()

    if len(window1) < WINDOW_SIZE or len(window2) < WINDOW_SIZE:
        print(f"[WARNING] Ventana incompleta en {t1_str} o {t2_str}, se omite.")
        continue

    # Plot
    plt.figure(figsize=(10, 5))
    plt.plot(range(WINDOW_SIZE), window1["close"], label=f"{t1_str}")
    plt.plot(range(WINDOW_SIZE), window2["close"], label=f"{t2_str}")
    plt.title(f"Ventanas similares (similitud={sim:.4f})")
    plt.xlabel("Minutos desde inicio de la ventana")
    plt.ylabel("Precio de cierre (close)")
    plt.legend()
    plt.tight_layout()
    fname = f"ventana_similar_{i+1:02d}.png"
    plt.savefig(fname)
    plt.close()
    print(f"[INFO] Guardado gráfico: {fname}")

print("\n✅ ¡Listo! Todos los gráficos fueron generados.")
