package org.mdp.kafka.cli;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mdp.kafka.def.KafkaConstants;

public class BitcoinPriceProducer {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: [csv_file] [kafka_topic] [speedup]");
            return;
        }

        String csvFile = args[0];
        String topic = args[1];
        int speedup = Integer.parseInt(args[2]);

        File file = new File(csvFile);
        RandomAccessFile raf = new RandomAccessFile(file, "r");

        Producer<String, String> producer = new KafkaProducer<>(KafkaConstants.PROPS);

        long fileLength = raf.length() - 1;
        StringBuilder line = new StringBuilder();
        int count = 0;
        boolean skipHeader = true;

        for (long pointer = fileLength; pointer >= 0; pointer--) {
            raf.seek(pointer);
            int readByte = raf.read();

            if (readByte == 0xA) { // LF: salto de línea
                if (line.length() > 0) {
                    String reversedLine = line.reverse().toString();

                    if (skipHeader) {
                        skipHeader = false; // saltar última línea que es la cabecera
                    } else {
                        producer.send(new ProducerRecord<>(topic, null, reversedLine));
                        count++;

                        if (count % 1000 == 0) {
                            System.out.println("Sent " + count + " messages so far...");
                        }

                        Thread.sleep(1000 / speedup);
                    }

                    line.setLength(0);
                }
            } else if (readByte != 0xD) { // ignorar CR
                line.append((char) readByte);
            }
        }

        // Última línea (la primera del archivo)
        if (line.length() > 0 && !skipHeader) {
            String reversedLine = line.reverse().toString();
            producer.send(new ProducerRecord<>(topic, null, reversedLine));
        }

        raf.close();
        producer.close();

        System.out.println("Finished sending all messages in reverse order. Total: " + count);
    }
}