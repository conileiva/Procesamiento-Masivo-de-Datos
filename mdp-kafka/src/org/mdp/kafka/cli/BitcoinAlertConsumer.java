package org.mdp.kafka.cli;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

public class BitcoinAlertConsumer {

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Uso: BitcoinAlertConsumer [kafka_topic] [window_size] [threshold_en_decimal]");
            return;
        }

        String topic = args[0];
        int windowSize;
        double threshold;

        try {
            windowSize = Integer.parseInt(args[1]);
            threshold = Double.parseDouble(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Error: window_size debe ser un entero y threshold un número decimal (ej: 0.01 para 1%)");
            return;
        }

        Properties props = KafkaConstants.PROPS;
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        LinkedList<Double> window = new LinkedList<>();
        LinkedList<String> fechas = new LinkedList<>();

        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy HH:mm");

        boolean alertaActiva = false;
        String tipoAlerta = "";
        String fechaInicioAlerta = "";
        double precioInicioAlerta = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String[] fields = record.value().split(",");

                    if (fields.length < 5) {
                        System.err.println("Formato incorrecto: " + record.value());
                        continue;
                    }

                    double price;
                    try {
                        price = Double.parseDouble(fields[4]);
                    } catch (NumberFormatException e) {
                        System.err.println("Error parseando precio: " + fields[4]);
                        continue;
                    }

                    String fecha = fields[0];

                    window.add(price);
                    fechas.add(fecha);

                    if (window.size() > windowSize) {
                        window.removeFirst();
                        fechas.removeFirst();
                    }

                    if (window.size() == windowSize) {
                        boolean subida = true;
                        boolean bajada = true;

                        for (int i = 1; i < window.size(); i++) {
                            double prev = window.get(i - 1);
                            double curr = window.get(i);
                            double change = (curr - prev) / prev;

                            if (change < threshold) subida = false;
                            if (change > -threshold) bajada = false;
                        }

                        double startPrice = window.getFirst();
                        double endPrice = window.getLast();
                        double percentChange = ((endPrice - startPrice) / startPrice) * 100;

                        String rawDate = fechas.getFirst();
                        String fechaFormateada;
                        try {
                            LocalDateTime ldt = LocalDateTime.parse(rawDate, inputFormatter);
                            fechaFormateada = ldt.atZone(ZoneId.of("America/Santiago")).format(outputFormatter);
                        } catch (Exception e) {
                            fechaFormateada = rawDate + " (no se pudo formatear)";
                        }

                        if (subida || bajada) {
                            if (!alertaActiva) {
                                alertaActiva = true;
                                tipoAlerta = subida ? "subida" : "bajada";
                                fechaInicioAlerta = fechaFormateada;
                                precioInicioAlerta = startPrice;

                                if (subida) {
                                    System.out.println("🚀 ALERTA DE SUBIDA INICIADA (VENTA)");
                                } else {
                                    System.out.println("📉 ALERTA DE BAJADA INICIADA (COMPRA)");
                                }

                                System.out.printf("📆 Fecha inicio: %s\n", fechaInicioAlerta);
                                System.out.printf("💲 Precio inicial: %.2f\n", precioInicioAlerta);
                            }

                            System.out.printf("📊 Precio actual: %.2f\n", endPrice);
                        } else if (alertaActiva) {
                            alertaActiva = false;
                            System.out.println("⚠️ FIN DE ALERTA");
                            System.out.printf("💲 Precio final: %.2f\n", endPrice);
                            double cambio = ((endPrice - precioInicioAlerta) / precioInicioAlerta) * 100;
                            System.out.printf("🔁 Cambio acumulado: %.2f%%\n\n\n", cambio);
                            tipoAlerta = "";
                        }
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
