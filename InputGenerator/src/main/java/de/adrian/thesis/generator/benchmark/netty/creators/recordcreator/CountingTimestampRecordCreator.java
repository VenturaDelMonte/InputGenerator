package de.adrian.thesis.generator.benchmark.netty.creators.recordcreator;

public class CountingTimestampRecordCreator implements RecordCreator {
    @Override
    public String createRecord(long number) {
        return System.currentTimeMillis() + "#" + number;
    }
}
