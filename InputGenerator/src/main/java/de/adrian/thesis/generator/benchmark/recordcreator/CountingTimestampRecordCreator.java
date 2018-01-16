package de.adrian.thesis.generator.benchmark.recordcreator;

public class CountingTimestampRecordCreator implements RecordCreator<String> {
    @Override
    public String createRecord(long number) {
        return System.currentTimeMillis() + "#" + number;
    }
}
