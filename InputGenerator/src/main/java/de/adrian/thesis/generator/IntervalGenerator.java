package de.adrian.thesis.generator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class IntervalGenerator {

    @Parameter(names = {"-n", "--name"}, description = "Name of the target RabbitMQ queue")
    private String queueName = "defaultQueue";

    @Parameter(names = {"-p", "--port"}, description = "Port of the RabbitMQ instance")
    private int port = 5672;

    @Parameter(names = {"-h", "--host"}, description = "Host name of the RabbitMQ instance")
    private String host = "localhost";

    @Parameter(names = {"-d", "--delay"}, description = "Delay between each new request")
    private int msDelay = 10;

    @Parameter(names = {"-f", "--file"}, description = "Input csv-file")
    private String file = "datasets/macroscopic-movement-01.csv";

    @Parameter(names = {"-l", "--maxLines"}, description = "Maximum number that should be processed from the input file")
    private long maxLines = -1;

    public static void main(String[] argv) throws Exception {
        IntervalGenerator generator = new IntervalGenerator();

        JCommander.newBuilder()
                .addObject(generator)
                .build()
                .parse(argv);

        generator.run();
    }

    private void run() throws Exception {

        System.out.printf("Connecting to %s:%d \"%s\" with file %s\n", host, port, queueName, file);

        Thread.sleep(5000);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);

        processFile(channel);

        channel.close();
        connection.close();
    }

    private void processFile(Channel channel) {

        try (BufferedReader input = new BufferedReader(new FileReader(file))) {

            String line;
            int counter = 0;

            System.out.printf("Starting to produce requests from file %s\n", file);

            while ((line = input.readLine()) != null) {
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
}
