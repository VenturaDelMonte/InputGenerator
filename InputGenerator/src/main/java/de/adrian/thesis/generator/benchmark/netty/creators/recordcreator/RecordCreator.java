package de.adrian.thesis.generator.benchmark.netty.creators.recordcreator;

public interface RecordCreator {
    /**
     * Creates a new stream record for the given number;
     *
     * @param number The current stream record number.
     * @return The parametrized return type
     */
    String createRecord(long number);
}
