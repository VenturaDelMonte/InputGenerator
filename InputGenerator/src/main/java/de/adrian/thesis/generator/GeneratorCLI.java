package de.adrian.thesis.generator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class GeneratorCLI {
    @Parameter(names = {"-c", "--class"}, required = true, description = "StringGenerator class, that should be started")
    private String generatorClass;

    @Parameter(names = {"-n", "--name"}, description = "Name of the target RabbitMQ queue")
    private String queueName = "defaultQueue";

    @Parameter(names = {"-p", "--port"}, description = "Port of the RabbitMQ instance")
    private int port = 5672;

    @Parameter(names = {"-h", "--host"}, description = "Host name of the RabbitMQ instance")
    private String host = "localhost";

    public static void main(String[] args) throws Exception {

        GeneratorCLI cli = new GeneratorCLI();
        cli.parseCLI(args);
        cli.startGenerator(args);
    }

    private void parseCLI(String[] args) {
        JCommander.newBuilder()
                .addObject(this)
                .acceptUnknownOptions(true)
                .build()
                .parse(args);
    }

    private void startGenerator(String[] args) throws InterruptedException {

        RabbitMQGenerator generator;

        switch (generatorClass) {
            case "numbers":
                generator = new GeneratorIncrementalCounting();
                break;
            case "numberedString":
                generator = new GeneratorNumberedStrings();
                break;
            case "csv":
                generator = new GeneratorFromCSV();
                break;
            default:
                throw new IllegalArgumentException("No StringGenerator with the name: " + generatorClass);
        }

        setupArgumentsForGenerator(args, generator);

        setupAndStartSending(generator);
    }

    private void setupArgumentsForGenerator(String[] args, RabbitMQGenerator generator) {
        JCommander.newBuilder()
                .addObject(generator)
                .acceptUnknownOptions(true)
                .build()
                .parse(args);
    }

    private void setupAndStartSending(RabbitMQGenerator generator) throws InterruptedException {
        System.out.printf("Connecting to %s:%d \"%s\"\n", host, port, queueName);

        Thread.sleep(5000);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);

        Channel channel = null;

        try (Connection connection = factory.newConnection()) {

            channel = connection.createChannel();

            channel.queueDeclare(queueName, false, false, false, null);

            generator.startSending(channel, queueName);

            channel.close();

        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.printf("Finished sending to %s:%d \"%s\"\n", host, port, queueName);
    }

    public interface RabbitMQGenerator {
        void startSending(Channel channel, String queueName) throws IOException, InterruptedException;
    }
}
