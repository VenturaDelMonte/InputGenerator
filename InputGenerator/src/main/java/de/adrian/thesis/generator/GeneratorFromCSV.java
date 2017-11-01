package de.adrian.thesis.generator;

import com.beust.jcommander.Parameter;
import com.rabbitmq.client.Channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class GeneratorFromCSV implements GeneratorCLI.RabbitMQGenerator {

    @Parameter(names = {"-d", "--delay"}, description = "Delay between each new request")
    private int msDelay = 10;

    @Parameter(names = {"-f", "--file"}, description = "Input csv-file")
    private String file = "datasets/macroscopic-movement-01.csv";

    @Parameter(names = {"-l", "--maxLines"}, description = "Maximum number that should be processed from the input file")
    private long maxLines = -1;

    private volatile boolean running = true;

    @Override
    public void startSending(Channel channel, String queueName) throws IOException, InterruptedException {
        processFile(channel, queueName);
    }

    private void processFile(Channel channel, String queueName) {

        try (BufferedReader input = new BufferedReader(new FileReader(file))) {

            String line;
            int counter = 0;

            System.out.printf("Starting to produce requests from file %s\n", file);

            while ((line = input.readLine()) != null && running) {
                channel.basicPublish("", queueName, null, line.getBytes());

                // Exit, when max lines to process has been reached
                if (++counter == maxLines) {
                    return;
                }

                Thread.sleep(msDelay);
            }

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopSending() {
        running = false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
