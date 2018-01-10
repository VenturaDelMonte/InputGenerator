package de.adrian.thesis.generator.benchmark.recordcreator;

public interface RecordCreator<T> {
    /**
     * Creates a new stream record for the given number;
     * @param number The current stream record number.
     * @return The parametrized return type
     */
    public T createRecord(long number);
}
