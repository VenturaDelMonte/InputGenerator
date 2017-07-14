import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class IntervalGenerator {

    @Parameter(names = { "-n", "--name" }, description = "Name of the target RabbitMQ queue")
    private String queueName = "defaultQueue";

    @Parameter(names = { "-p", "--port" }, description = "Port of the RabbitMQ instance")
    private int port = 5672;

    @Parameter(names = { "-h", "--host" }, description = "Host name of the RabbitMQ instance")
    private String host = "localhost";

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

        Thread.sleep(10000);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);
        String message = "Hello World!";

        for (int i = 0; i < 1000; i++) {
            channel.basicPublish("", queueName, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }

        channel.close();
        connection.close();
    }
}
