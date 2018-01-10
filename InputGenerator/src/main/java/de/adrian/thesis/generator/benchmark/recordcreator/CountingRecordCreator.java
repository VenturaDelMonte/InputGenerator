package de.adrian.thesis.generator.benchmark.recordcreator;

public class CountingRecordCreator implements RecordCreator<String> {
    @Override
    public String createRecord(long number) {
        return "Element#" + number;
    }
}
