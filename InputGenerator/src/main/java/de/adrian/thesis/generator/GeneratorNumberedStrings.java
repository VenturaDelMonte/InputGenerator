package de.adrian.thesis.generator;

public class GeneratorNumberedStrings extends AbstractRabbitMQGenerator {
    GeneratorNumberedStrings() {
        super(id -> "Messages - " + id);
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
