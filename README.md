# Procesamiento-Masivo-de-Datos

# Grupo 1
- Matías Godoy
- Constanza Leiva
- Francisco Márquez
- Delaney Tello

Los códigos encargados de la simulación y detección de anomalías de precios de Bitcoin son:
- mdp-kafka/src/cli/BitcoinPriceProducer.java — Productor de datos de precios Bitcoin para Kafka
- mdp-kafka/src/cli/BitcoinAlertConsumer.java — Consumidor que detecta anomalías en los precios

El código encargado de realizar la detección de comportamientos similares se encuentra en:
- Proyecto/Proyecto.py

Los resultados de las comparaciones se encuentran en dos carpetas:
- Proyecto/output0/ — Contiene los resultados correspondientes a las comparaciones de todas las ventanas entre sí (modo 0)
- Proyecto/output/ — Contiene los resultados correspondientes a las comparaciones de la última ventana contra ventanas anteriores (modo 1).

Los gráficos resultantes de las comparaciones se encuentran en dos carpetas:
- plots_similares0/ — Contiene los gráficos correspondientes a las comparaciones de todas las ventanas entre sí (modo 0)
- plots_similares/ — Contiene los gráficos correspondientes a las comparaciones de la última ventana contra ventanas anteriores (modo 1).

# Ejecución

Para ejecutar las alertas se usan los siguientes comandos:

## Para producer/consumer

```java -cp Bitcoin.jar org.mdp.kafka.cli.BitcoinPriceAlert <topic> <speedup>```

```java -cp Bitcoin.jar org.mdp.kafka.cli.BitcoinAlertConsumer <topic> <tamaño de ventana> <threshold>``` 

## Para similitud de comportamientos

```Proyecto.py <modo>``` 

con modo siendo 0 o 1 dependiendo el modo que se quiera ejecutar.

