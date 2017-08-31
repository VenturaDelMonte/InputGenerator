package de.adrian.thesis.generator;

public class GeneratorNumberedStrings extends AbstractRabbitMQGenerator {
    GeneratorNumberedStrings() {
        super(id -> "Messages - " + id);
    }
}
