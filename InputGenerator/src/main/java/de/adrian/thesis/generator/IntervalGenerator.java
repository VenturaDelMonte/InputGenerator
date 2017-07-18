package de.adrian.thesis.generator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class IntervalGenerator {

    @Parameter(names = {"-n", "--name"}, description = "Name of the target RabbitMQ queue")
    private String queueName = "defaultQueue";

    @Parameter(names = {"-p", "--port"}, description = "Port of the RabbitMQ instance")
    private int port = 5672;

    @Parameter(names = {"-h", "--host"}, description = "Host name of the RabbitMQ instance")
    private String host = "localhost";

    @Parameter(names = {"-d", "--delay"}, description = "Delay between each new request")
    private int msDelay = 100;

    @Parameter(names = {"-m", "--maxMessages"}, description = "Maximum number of messages, that should be generated")
    private long maxMessages = -1;

    public static void main(String[] argv) throws Exception {
        IntervalGenerator generator = new IntervalGenerator();

        JCommander.newBuilder()
                .addObject(generator)
                .build()
                .parse(argv);

        generator.run();
    }

    private void run() throws Exception {

        System.out.printf("Connecting to %s:%d \"%s\"\n", host, port, queueName);

        Thread.sleep(5000);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, true, false, false, null);
        String message;

        System.out.printf("Starting to produce messages with delay %d\n", msDelay);

        long messagesSent = 0;

        while (true) {

            message = "Message - " + messagesSent;

            channel.basicPublish("", queueName, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            // Exit, when max number of messages has been reached
            if (++messagesSent == maxMessages) {
                break;
            }

            Thread.sleep(msDelay);
        }

        channel.close();
        connection.close();
    }
}
