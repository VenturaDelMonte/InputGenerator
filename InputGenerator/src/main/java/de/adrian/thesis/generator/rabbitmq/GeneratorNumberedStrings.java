package de.adrian.thesis.generator.rabbitmq;

public class GeneratorNumberedStrings extends AbstractRabbitMQGenerator {
    GeneratorNumberedStrings() {
        super(id -> "Messages - " + id);
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
