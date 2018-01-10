package de.adrian.thesis.generator.rabbitmq;

import com.beust.jcommander.Parameter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import java.io.IOException;

class AbstractRabbitMQGenerator implements GeneratorCLI.RabbitMQGenerator {

    @Parameter(names = {"-d", "--delay"}, description = "Delay between each new request")
    private int msDelay = 100;

    @Parameter(names = {"-m", "--maxMessages"}, description = "Maximum number of messages, that should be generated")
    private long maxMessages = -1;

    @Parameter(names = {"-co", "--correlationIDs"}, description = "Whether RabbitMQ should send correlationIDs (Necessary for checkpointing).", arity = 1)
    private boolean useCorrelationIds = true;

    @Parameter(names = {"-e", "--every"}, description = "Only print every x-th 'message send' statement. Set to '1' to print every successful sent.")
    private int modulePrint = 50;

    private final StringGenerator stringGenerator;

    private volatile boolean running = false;

    AbstractRabbitMQGenerator(StringGenerator stringGenerator) {
        this.stringGenerator = stringGenerator;
    }

    @Override
    public void startSending(Channel channel, String queueName) throws IOException, InterruptedException {
        String message;

        System.out.printf("Starting to produce messages with delay %d (correlationIDs: %b)\n",
                msDelay, useCorrelationIds);

        long messagesSent = 0;
        running = true;

        while (running) {

            AMQP.BasicProperties props;
            if (useCorrelationIds) {
                // String corrId = java.util.UUID.randomUUID().toString();
                props = new AMQP.BasicProperties().builder().correlationId(String.valueOf(messagesSent)).build();
            } else {
                props = null;
            }


            message = stringGenerator.generateStringFromMessageID(messagesSent);

            channel.basicPublish("", queueName, props, message.getBytes());

            // If delay is smaller than certain threshold, printing to std.out will block sending unnecessarily
            if (msDelay <= 50) {
                if ((messagesSent % modulePrint) == 0) {
                    System.out.println("[x] Sent '" + message + "'");
                }
            } else {
                System.out.println("[x] Sent '" + message + "'");
            }

            // Exit, when max number of messages has been reached
            if (++messagesSent == maxMessages) {
                break;
            }

            Thread.sleep(msDelay);
        }
    }

    @Override
    public void stopSending() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public interface StringGenerator {
        String generateStringFromMessageID(long id);
    }
}
