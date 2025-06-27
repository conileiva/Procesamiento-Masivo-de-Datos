package org.mdp.kafka.sim;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BitcoinStream implements Runnable {

    BufferedReader csvReader;
    Producer<String, String> producer;
    String topic;
    int speedUp;

    public BitcoinStream(BufferedReader csvReader, Producer<String, String> producer, String topic, int speedUp) {
        this.csvReader = csvReader;
        this.producer = producer;
        this.topic = topic;
        this.speedUp = speedUp;
    }

    @Override
    public void run() {
        try {
            String header = csvReader.readLine(); // skip header
            String line;
            long previousTimestamp = -1;

            while ((line = csvReader.readLine()) != null) {
                String[] fields = line.split(",");
                long currentTimestamp = Long.parseLong(fields[0]);

                if (previousTimestamp != -1) {
                    long timeDiff = currentTimestamp - previousTimestamp;
                    long sleepTime = timeDiff / speedUp;

                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                }

                producer.send(new ProducerRecord<>(topic, null, line));
                System.out.println("Published: " + line);

                previousTimestamp = currentTimestamp;
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}