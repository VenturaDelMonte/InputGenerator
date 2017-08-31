package de.adrian.thesis.generator;

/**
 * Sends the current message number as string to RabbitMQ.
 */
public class GeneratorIncrementalCounting extends AbstractRabbitMQGenerator {
    GeneratorIncrementalCounting() {
        super(String::valueOf);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
